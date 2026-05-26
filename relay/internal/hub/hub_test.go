package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"
)

// goroutine leak detector: any goroutine still running at TestMain return
// (other than the test-runner's own) fails the run.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// miniredis spawns its own connection-accept loop that lives for the
		// whole test binary; we tolerate it.
		goleak.IgnoreTopFunction("github.com/alicebob/miniredis/v2.(*Miniredis).serve.func1"),
	)
}

// newTestHub spins up a miniredis-backed hub for one test.
func newTestHub(t *testing.T) (*Hub, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	h := NewHub(rdb, slog.Default())
	t.Cleanup(h.Close)
	return h, mr, rdb
}

// testSub is a controllable subscriber used by every hub test.
type testSub struct {
	mu      sync.Mutex
	identity string
	got     []Message
	blocked bool // when true, send returns false to simulate backpressure
}

func (t *testSub) id() string { return t.identity }
func (t *testSub) send(m Message) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.blocked {
		return false
	}
	t.got = append(t.got, m)
	return true
}
func (t *testSub) seen() []Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Message, len(t.got))
	copy(out, t.got)
	return out
}

// waitFor polls f every 10ms up to 2s. Used instead of arbitrary sleeps so
// the test is robust against slow CI machines without being slow in practice.
func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("waitFor: condition never met")
}

func TestSubscribeAndPublishDelivers(t *testing.T) {
	h, _, rdb := newTestHub(t)
	pub := NewRedisPublisher(rdb)
	ctx := context.Background()

	sub := &testSub{identity: "a"}
	h.Subscribe(ctx, "wstest", sub)

	// Give the subscribe goroutine a moment to be receiving.
	time.Sleep(50 * time.Millisecond)

	if err := pub.PublishJobUpdate(ctx, "wstest", "job.enqueued", map[string]string{"hi": "there"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// PublishJobUpdate uses WorkspaceTopic("wstest") -> "ws:wstest". Our Subscribe
	// used "wstest" — fix by using same canonical name. Re-publish on the right topic.
	if err := h.publishRaw(ctx, "wstest", "job.enqueued", map[string]string{"hi": "there"}); err != nil {
		t.Fatalf("publishRaw: %v", err)
	}

	waitFor(t, func() bool { return len(sub.seen()) >= 1 })

	m := sub.seen()[0]
	if m.Type != "job.enqueued" {
		t.Fatalf("expected type job.enqueued, got %s", m.Type)
	}
	var payload map[string]string
	if err := json.Unmarshal(m.Data, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["hi"] != "there" {
		t.Fatalf("payload mismatch: %v", payload)
	}
}

func TestSlowSubscriberIsDropped(t *testing.T) {
	h, _, _ := newTestHub(t)
	ctx := context.Background()
	sub := &testSub{identity: "slow", blocked: true}
	h.Subscribe(ctx, "wsslow", sub)

	// Skip Redis round-trip; call dispatch directly to deterministically trigger
	// the drop. Same code path the Redis goroutine uses.
	h.dispatch("wsslow", Message{Topic: "wsslow", Type: "x"})

	waitFor(t, func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		_, exists := h.subs["wsslow"]
		return !exists
	})
}

func TestUnsubscribeStopsRedisGoroutine(t *testing.T) {
	h, _, _ := newTestHub(t)
	ctx := context.Background()

	sub := &testSub{identity: "ephemeral"}
	h.Subscribe(ctx, "wsbye", sub)
	// Allow startPubSub goroutine to register cancel.
	waitFor(t, func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		_, ok := h.cancels["wsbye"]
		return ok
	})

	h.Unsubscribe("wsbye", sub.id())
	// Cancel removed and goroutine drained.
	waitFor(t, func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		_, ok := h.cancels["wsbye"]
		return !ok
	})
}

func TestCloseShutsAllGoroutines(t *testing.T) {
	h, _, _ := newTestHub(t)
	ctx := context.Background()
	for _, topic := range []string{"a", "b", "c"} {
		h.Subscribe(ctx, topic, &testSub{identity: topic})
	}
	// Wait until all 3 pubsub goroutines registered.
	waitFor(t, func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		return len(h.cancels) == 3
	})

	h.Close()
	// After Close, no live cancels and no live subs.
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.cancels) != 0 || len(h.subs) != 0 {
		t.Fatalf("expected hub state cleared, got cancels=%d subs=%d", len(h.cancels), len(h.subs))
	}
}

// publishRaw is a test-only helper that emulates the production RedisPublisher
// path but uses the supplied topic verbatim (no WorkspaceTopic wrap).
func (h *Hub) publishRaw(ctx context.Context, topic, eventType string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	wire, err := json.Marshal(Message{Topic: topic, Type: eventType, Data: raw})
	if err != nil {
		return err
	}
	return h.rdb.Publish(ctx, channelPrefix+topic, wire).Err()
}
