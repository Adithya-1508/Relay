package hub

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

// Publisher is the producer-side abstraction job.Service uses to broadcast
// state changes. Implementations:
//   - RedisPublisher: PUBLISH to Redis, which every API instance's Hub
//     subscriber goroutine receives and fans out
//   - NoopPublisher: silently drops; used when no Redis is wired (tests)
type Publisher interface {
	PublishJobUpdate(ctx context.Context, workspaceID, eventType string, payload any) error
}

// RedisPublisher publishes events to the Redis pub/sub channel for a topic.
// No Hub instance required — used by the worker process which only emits.
type RedisPublisher struct {
	rdb *redis.Client
}

// NewRedisPublisher binds a publisher to a Redis client.
func NewRedisPublisher(rdb *redis.Client) *RedisPublisher { return &RedisPublisher{rdb: rdb} }

// PublishJobUpdate publishes to the workspace topic. eventType is one of
// "job.enqueued", "job.started", "job.succeeded", "job.failed", "job.dead".
func (p *RedisPublisher) PublishJobUpdate(ctx context.Context, workspaceID, eventType string, payload any) error {
	if p == nil || p.rdb == nil {
		return nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	msg := Message{
		Topic: WorkspaceTopic(workspaceID),
		Type:  eventType,
		Data:  raw,
	}
	wire, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return p.rdb.Publish(ctx, channelPrefix+WorkspaceTopic(workspaceID), wire).Err()
}

// NoopPublisher discards every event. Convenient for tests.
type NoopPublisher struct{}

// PublishJobUpdate is a no-op.
func (NoopPublisher) PublishJobUpdate(_ context.Context, _, _ string, _ any) error { return nil }

// WorkspaceTopic builds the canonical pub/sub topic name for a workspace.
func WorkspaceTopic(workspaceID string) string { return "ws:" + workspaceID }
