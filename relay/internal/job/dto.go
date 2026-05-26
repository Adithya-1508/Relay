package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// CreatePipelineRequest is the body for POST /v1/pipelines.
type CreatePipelineRequest struct {
	Name        string `json:"name"        binding:"required,min=1,max=80"`
	Slug        string `json:"slug"        binding:"required,min=2,max=80"`
	Description string `json:"description" binding:"max=400"`
}

// EnqueueJobRequest is the body for POST /v1/pipelines/:slug/jobs.
type EnqueueJobRequest struct {
	Kind           string          `json:"kind"            binding:"required,min=1,max=64"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty" binding:"omitempty,max=200"`
	MaxAttempts    int             `json:"max_attempts"    binding:"omitempty,min=1,max=50"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty"`
	DependsOn      []uuid.UUID     `json:"depends_on,omitempty"`
}

// ListJobsRequest is the query for GET /v1/jobs.
type ListJobsRequest struct {
	State    string     `form:"state"`
	Limit    int        `form:"limit"`
	BeforeAt *time.Time `form:"before"`
	BeforeID *uuid.UUID `form:"before_id"`
}

// JobsPage is a keyset-paginated list of jobs plus a cursor to the next page.
type JobsPage struct {
	Jobs       []Job   `json:"jobs"`
	NextBefore *string `json:"next_before,omitempty"`
	NextID     *string `json:"next_before_id,omitempty"`
}
