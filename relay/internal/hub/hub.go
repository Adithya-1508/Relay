// Package hub provides a process-local WebSocket fan-out backed by Redis
// pub/sub, so multiple API instances all deliver real-time events to clients
// subscribed to the same workspace topic.
//
// Wire layout:
//
//	worker / api ──▶ publisher.Publish(topic, msg) ──▶ Redis PUBLISH
//	                                                       │
//	                                                       ▼
//	                                              (every api instance)
//	                                              Redis SUBSCRIBE
//	                                                       │
//	                                                       ▼
//	                                              Hub.dispatch(topic, msg)
//	                                                       │
//	                                                       ▼  (local clients
//	                                              client.send channel        subscribed
//	                                                       │                  to topic)
//	                                                       ▼
//	                                              WebSocket frame to user
package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"
)

// channelPrefix namespaces all Relay pub/sub channels.
const channelPrefix = "relay:hub:"

// Message is what the hub fans out. Wire JSON is `{"topic":...,"type":...,"data":...}`.
type Message struct {
	Topic string          `json:"topic"`
	Type  string          `json:"type"`
	Data  json.RawMessage `json:"data,omitempty"`
}

// subscriber is the in-process side of a connection — anything that wants to
// receive Messages for a topic. WebSocket clients implement this, tests can
// implement it directly.
type subscriber interface {
	send(m Message) bool // returns false if client should be dropped
	id() string
}

// Hub multiplexes Redis pub/sub fan-out to per-process WebSocket clients.
// Goroutine model:
//   - one background goroutine per Redis subscription (lazy, started on first
//     subscriber for a topic)
//   - one writer goroutine per connected client (lives in client.go)
//
// Hub itself stores no goroutines for ordinary subscribe/unsubscribe; those
// operations are short critical sections guarded by mu.
type Hub struct {
	rdb *redis.Client
	log *slog.Logger

	mu         sync.RWMutex
	subs       map[string]map[string]subscriber  // topic -> id -> subscriber
	cancels    map[string]context.CancelFunc     // topic -> cancel for pubsub goroutine
	closing    bool
	wg         sync.WaitGroup
}

// NewHub builds a Hub bound to a Redis client.
func NewHub(rdb *redis.Client, log *slog.Logger) *Hub {
	if log == nil {
		log = slog.Default()
	}
	return &Hub{
		rdb:     rdb,
		log:     log,
		subs:    map[string]map[string]subscriber{},
		cancels: map[string]context.CancelFunc{},
	}
}

// Subscribe registers s for messages on topic. The hub may start a Redis
// subscriber goroutine on the first subscriber for a given topic.
func (h *Hub) Subscribe(ctx context.Context, topic string, s subscriber) {
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		return
	}
	bucket, ok := h.subs[topic]
	if !ok {
		bucket = map[string]subscriber{}
		h.subs[topic] = bucket
	}
	bucket[s.id()] = s
	startPubSub := len(bucket) == 1
	h.mu.Unlock()

	if startPubSub {
		h.startPubSub(ctx, topic)
	}
}

// Unsubscribe removes s from topic. Stops the Redis subscriber goroutine when
// the last subscriber for that topic leaves.
func (h *Hub) Unsubscribe(topic, id string) {
	h.mu.Lock()
	bucket, ok := h.subs[topic]
	if !ok {
		h.mu.Unlock()
		return
	}
	delete(bucket, id)
	stop := false
	if len(bucket) == 0 {
		delete(h.subs, topic)
		if cancel, hasCancel := h.cancels[topic]; hasCancel {
			delete(h.cancels, topic)
			defer cancel()
			stop = true
		}
	}
	h.mu.Unlock()
	_ = stop
}

// startPubSub launches the Redis SUBSCRIBE goroutine for a topic. Idempotent
// via the cancels map check.
func (h *Hub) startPubSub(parentCtx context.Context, topic string) {
	channel := channelPrefix + topic

	h.mu.Lock()
	if _, exists := h.cancels[topic]; exists {
		h.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parentCtx)
	h.cancels[topic] = cancel
	h.wg.Add(1)
	h.mu.Unlock()

	go func() {
		defer h.wg.Done()
		ps := h.rdb.Subscribe(ctx, channel)
		defer ps.Close()

		// Wait for confirmation so misconfig is surfaced immediately.
		if _, err := ps.Receive(ctx); err != nil {
			h.log.Warn("hub subscribe failed", "topic", topic, "error", err)
			return
		}
		ch := ps.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var m Message
				if err := json.Unmarshal([]byte(msg.Payload), &m); err != nil {
					h.log.Warn("hub decode payload", "topic", topic, "error", err)
					continue
				}
				h.dispatch(topic, m)
			}
		}
	}()
}

// dispatch hands a message to every subscriber for topic. Subscribers whose
// send() returns false are queued for removal — but the actual delete waits
// until we have released the read lock to avoid lock-promotion deadlocks.
func (h *Hub) dispatch(topic string, m Message) {
	h.mu.RLock()
	bucket, ok := h.subs[topic]
	if !ok {
		h.mu.RUnlock()
		return
	}
	var toDrop []string
	for id, s := range bucket {
		if !s.send(m) {
			toDrop = append(toDrop, id)
		}
	}
	h.mu.RUnlock()

	for _, id := range toDrop {
		h.Unsubscribe(topic, id)
	}
}

// Close stops every Redis subscriber goroutine and waits for them to exit.
// Safe to call once during graceful shutdown.
func (h *Hub) Close() {
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		return
	}
	h.closing = true
	cancels := h.cancels
	h.cancels = map[string]context.CancelFunc{}
	h.subs = map[string]map[string]subscriber{}
	h.mu.Unlock()

	for _, c := range cancels {
		c()
	}
	h.wg.Wait()
}
