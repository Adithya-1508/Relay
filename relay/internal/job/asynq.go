package job

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

// TaskType is the Asynq task type the worker listens on.
const TaskType = "relay:job"

// TaskPayload is the (small) payload carried in the Asynq task. The full job
// row lives in Postgres — the worker re-fetches it by ID. This keeps Redis
// memory bounded regardless of job payload size.
type TaskPayload struct {
	JobID uuid.UUID `json:"job_id"`
}

// Enqueuer is the interface the service layer uses to push jobs onto the
// queue. Defined as an interface so tests can inject a fake.
type Enqueuer interface {
	Enqueue(ctx context.Context, jobID uuid.UUID, opts EnqueueOpts) error
	Close() error
}

// EnqueueOpts configures the Asynq task wrap.
type EnqueueOpts struct {
	MaxRetry  int
	ProcessIn time.Duration // delay before first attempt; 0 = now
	Queue     string        // optional queue name; defaults to "default"
}

// AsynqEnqueuer is the production Enqueuer backed by an asynq.Client.
type AsynqEnqueuer struct {
	client *asynq.Client
}

// NewAsynqEnqueuer wires an Asynq client to the supplied Redis address.
func NewAsynqEnqueuer(redisAddr, redisPassword string) *AsynqEnqueuer {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr, Password: redisPassword})
	return &AsynqEnqueuer{client: client}
}

// Enqueue pushes a single job onto the queue.
func (e *AsynqEnqueuer) Enqueue(ctx context.Context, jobID uuid.UUID, opts EnqueueOpts) error {
	payload, err := json.Marshal(TaskPayload{JobID: jobID})
	if err != nil {
		return fmt.Errorf("marshal task payload: %w", err)
	}

	asynqOpts := []asynq.Option{}
	if opts.MaxRetry > 0 {
		asynqOpts = append(asynqOpts, asynq.MaxRetry(opts.MaxRetry))
	}
	if opts.ProcessIn > 0 {
		asynqOpts = append(asynqOpts, asynq.ProcessIn(opts.ProcessIn))
	}
	if opts.Queue != "" {
		asynqOpts = append(asynqOpts, asynq.Queue(opts.Queue))
	}
	// Asynq dedup option keys the queue side by jobID — duplicate enqueues of
	// the same job in a short window collapse to one task.
	asynqOpts = append(asynqOpts, asynq.TaskID(jobID.String()), asynq.Retention(24*time.Hour))

	task := asynq.NewTask(TaskType, payload)
	if _, err := e.client.EnqueueContext(ctx, task, asynqOpts...); err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	return nil
}

// Close shuts down the underlying Asynq client.
func (e *AsynqEnqueuer) Close() error {
	return e.client.Close()
}
