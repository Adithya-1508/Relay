package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// State is the discrete lifecycle phase of a job. Values match the CHECK
// constraint in 000002_jobs.up.sql.
type State string

const (
	StateBlocked   State = "blocked" // waiting on unsatisfied job_dependencies
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"   // failed but eligible for retry
	StateDead      State = "dead"     // exhausted retries — manual intervention
	StateCancelled State = "cancelled"
)

// Pipeline mirrors the pipelines table.
type Pipeline struct {
	ID          uuid.UUID `json:"id"`
	WorkspaceID uuid.UUID `json:"workspace_id"`
	Name        string    `json:"name"`
	Slug        string    `json:"slug"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Job mirrors the jobs table.
type Job struct {
	ID             uuid.UUID       `json:"id"`
	PipelineID     uuid.UUID       `json:"pipeline_id"`
	WorkspaceID    uuid.UUID       `json:"workspace_id"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	State          State           `json:"state"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	Version        int64           `json:"version"`
	ScheduledAt    time.Time       `json:"scheduled_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// Event is an append-only audit row recording a state transition or
// informational event.
type Event struct {
	ID        int64           `json:"id"`
	JobID     uuid.UUID       `json:"job_id"`
	Event     string          `json:"event"`
	FromState *State          `json:"from_state,omitempty"`
	ToState   *State          `json:"to_state,omitempty"`
	Message   string          `json:"message"`
	Metadata  json.RawMessage `json:"metadata"`
	CreatedAt time.Time       `json:"created_at"`
}
