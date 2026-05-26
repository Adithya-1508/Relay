package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Service-level errors. Handler maps these to HTTP error codes.
var (
	ErrPipelineNotFound = errors.New("pipeline not found")
	ErrPipelineExists   = errors.New("pipeline already exists")
	ErrJobNotFound      = errors.New("job not found")
	ErrUnknownKind      = errors.New("unknown job kind")
	ErrBadDependency    = errors.New("invalid job dependency")
)

// Publisher is the broadcast contract used to notify connected clients of
// job state changes. Defined here (not imported from internal/hub) so the
// job package has no dependency on hub. hub.HubPublisher satisfies this
// interface structurally.
type Publisher interface {
	PublishJobUpdate(ctx context.Context, workspaceID, eventType string, payload any) error
}

// Service is the business-logic surface of the job domain.
type Service struct {
	repo               Repository
	enqueuer           Enqueuer
	registry           *Registry
	publisher          Publisher
	log                *slog.Logger
	clock              func() time.Time
	defaultMaxAttempts int
}

// Config wires Service dependencies.
type Config struct {
	Repository         Repository
	Enqueuer           Enqueuer
	Registry           *Registry
	Publisher          Publisher
	Logger             *slog.Logger
	Clock              func() time.Time
	DefaultMaxAttempts int
}

// NewService builds a Service. Clock defaults to time.Now, DefaultMaxAttempts
// to 5 when zero/negative.
func NewService(cfg Config) *Service {
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	if cfg.DefaultMaxAttempts <= 0 {
		cfg.DefaultMaxAttempts = 5
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		repo:               cfg.Repository,
		enqueuer:           cfg.Enqueuer,
		registry:           cfg.Registry,
		publisher:          cfg.Publisher,
		log:                log,
		clock:              clock,
		defaultMaxAttempts: cfg.DefaultMaxAttempts,
	}
}

// publish is a nil-safe helper. The Publisher field is optional; tests omit it.
func (s *Service) publish(ctx context.Context, workspaceID uuid.UUID, eventType string, payload any) {
	if s.publisher == nil {
		return
	}
	if err := s.publisher.PublishJobUpdate(ctx, workspaceID.String(), eventType, payload); err != nil {
		s.log.Warn("publish job update", "type", eventType, "error", err)
	}
}

// CreatePipeline registers a new pipeline in the given workspace.
func (s *Service) CreatePipeline(ctx context.Context, workspaceID uuid.UUID, req CreatePipelineRequest) (Pipeline, error) {
	p := Pipeline{
		WorkspaceID: workspaceID,
		Name:        req.Name,
		Slug:        strings.ToLower(req.Slug),
		Description: req.Description,
	}
	created, err := s.repo.CreatePipeline(ctx, p)
	if err != nil {
		if errors.Is(err, ErrDuplicate) {
			return Pipeline{}, ErrPipelineExists
		}
		return Pipeline{}, err
	}
	return created, nil
}

// ListPipelines returns all pipelines in the workspace.
func (s *Service) ListPipelines(ctx context.Context, workspaceID uuid.UUID) ([]Pipeline, error) {
	return s.repo.ListPipelines(ctx, workspaceID)
}

// EnqueueJob creates a Job row in Postgres and pushes an Asynq task pointing
// at it. Idempotency: if a job with the same (pipeline, idempotency_key)
// already exists, returns the existing job without re-enqueuing.
func (s *Service) EnqueueJob(ctx context.Context, workspaceID uuid.UUID, pipelineSlug string, req EnqueueJobRequest) (Job, error) {
	if _, ok := s.registry.Lookup(req.Kind); !ok {
		return Job{}, ErrUnknownKind
	}

	pipeline, err := s.repo.FindPipelineBySlug(ctx, workspaceID, pipelineSlug)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Job{}, ErrPipelineNotFound
		}
		return Job{}, err
	}

	maxAttempts := req.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = s.defaultMaxAttempts
	}

	now := s.clock()
	scheduled := now
	if req.ScheduledAt != nil && req.ScheduledAt.After(now) {
		scheduled = *req.ScheduledAt
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}

	candidate := Job{
		PipelineID:     pipeline.ID,
		WorkspaceID:    workspaceID,
		IdempotencyKey: req.IdempotencyKey,
		Kind:           req.Kind,
		Payload:        payload,
		State:          StatePending,
		MaxAttempts:    maxAttempts,
		ScheduledAt:    scheduled,
	}

	// Validate deps belong to same workspace before creating the row, so we
	// don't leave a half-built job around.
	if err := s.validateDeps(ctx, workspaceID, req.DependsOn); err != nil {
		return Job{}, err
	}

	created, isNew, err := s.repo.CreateJob(ctx, candidate)
	if err != nil {
		return Job{}, err
	}
	if !isNew {
		// Idempotency hit: do not re-enqueue. Just return what's already there.
		return created, nil
	}

	// Insert dependency rows; if any dep is not yet succeeded, the job stays
	// blocked and won't be enqueued to Asynq. The completing dep will release it.
	if len(req.DependsOn) > 0 {
		if err := s.repo.InsertDependencies(ctx, created.ID, req.DependsOn); err != nil {
			return Job{}, fmt.Errorf("insert dependencies: %w", err)
		}
		unresolved, err := s.repo.UnresolvedDeps(ctx, created.ID)
		if err != nil {
			return Job{}, err
		}
		if unresolved > 0 {
			blocked := StateBlocked
			updated, err := s.repo.TransitionJob(ctx, created.ID, created.Version, JobPatch{State: &blocked})
			if err == nil {
				created = updated
			}
			_ = s.repo.AppendEvent(ctx, Event{
				JobID:   created.ID,
				Event:   "blocked",
				ToState: pState(StateBlocked),
				Message: fmt.Sprintf("waiting on %d dependencies", unresolved),
			})
			s.publish(ctx, workspaceID, "job.blocked", created)
			return created, nil
		}
	}

	if err := s.repo.AppendEvent(ctx, Event{
		JobID:   created.ID,
		Event:   "enqueued",
		ToState: pState(StatePending),
		Message: "job enqueued",
	}); err != nil {
		s.log.Warn("append enqueue event", "job_id", created.ID, "error", err)
	}

	delay := scheduled.Sub(now)
	if delay < 0 {
		delay = 0
	}
	if err := s.enqueuer.Enqueue(ctx, created.ID, EnqueueOpts{
		MaxRetry:  maxAttempts,
		ProcessIn: delay,
	}); err != nil {
		return Job{}, fmt.Errorf("enqueue asynq task: %w", err)
	}

	s.publish(ctx, workspaceID, "job.enqueued", created)
	return created, nil
}

// validateDeps returns ErrBadDependency if any dep id is missing or belongs to
// a different workspace. Returns nil for an empty slice.
func (s *Service) validateDeps(ctx context.Context, workspaceID uuid.UUID, depIDs []uuid.UUID) error {
	for _, id := range depIDs {
		dep, err := s.repo.FindJobByID(ctx, id)
		if err != nil {
			return fmt.Errorf("%w: dep %s not found", ErrBadDependency, id)
		}
		if dep.WorkspaceID != workspaceID {
			return fmt.Errorf("%w: dep %s belongs to a different workspace", ErrBadDependency, id)
		}
	}
	return nil
}

// releaseDependents finds every blocked job that depends on parentID, checks
// whether all its other dependencies are now satisfied, and if so transitions
// the dependent to pending and enqueues the Asynq task.
func (s *Service) releaseDependents(ctx context.Context, parent Job) {
	deps, err := s.repo.ListDependents(ctx, parent.ID)
	if err != nil {
		s.log.Warn("list dependents", "parent_id", parent.ID, "error", err)
		return
	}
	for _, dep := range deps {
		if dep.State != StateBlocked {
			continue
		}
		unresolved, err := s.repo.UnresolvedDeps(ctx, dep.ID)
		if err != nil {
			s.log.Warn("check deps", "job_id", dep.ID, "error", err)
			continue
		}
		if unresolved > 0 {
			continue
		}
		pending := StatePending
		released, err := s.repo.TransitionJob(ctx, dep.ID, dep.Version, JobPatch{State: &pending})
		if err != nil {
			s.log.Warn("release dependent", "job_id", dep.ID, "error", err)
			continue
		}
		_ = s.repo.AppendEvent(ctx, Event{
			JobID:     released.ID,
			Event:     "released",
			FromState: pState(StateBlocked),
			ToState:   pState(StatePending),
			Message:   "all dependencies satisfied",
		})
		if err := s.enqueuer.Enqueue(ctx, released.ID, EnqueueOpts{MaxRetry: released.MaxAttempts}); err != nil {
			s.log.Warn("enqueue released dep", "job_id", released.ID, "error", err)
			continue
		}
		s.publish(ctx, released.WorkspaceID, "job.enqueued", released)
	}
}

// cancelDependents marks every blocked dependent of parent as cancelled
// (upstream failure means they can never run).
func (s *Service) cancelDependents(ctx context.Context, parent Job, reason string) {
	deps, err := s.repo.ListDependents(ctx, parent.ID)
	if err != nil {
		s.log.Warn("list dependents on cancel", "parent_id", parent.ID, "error", err)
		return
	}
	for _, dep := range deps {
		if dep.State != StateBlocked {
			continue
		}
		cancelled := StateCancelled
		msg := reason
		updated, err := s.repo.TransitionJob(ctx, dep.ID, dep.Version, JobPatch{State: &cancelled, LastError: &msg})
		if err != nil {
			s.log.Warn("cancel dependent", "job_id", dep.ID, "error", err)
			continue
		}
		_ = s.repo.AppendEvent(ctx, Event{
			JobID:     updated.ID,
			Event:     "cancelled",
			FromState: pState(StateBlocked),
			ToState:   pState(StateCancelled),
			Message:   reason,
		})
		s.publish(ctx, updated.WorkspaceID, "job.cancelled", updated)
		// Propagate cascade: dependents of this cancelled job also cancel.
		s.cancelDependents(ctx, updated, reason)
	}
}

// GetJob returns a workspace-scoped job by id.
func (s *Service) GetJob(ctx context.Context, workspaceID, jobID uuid.UUID) (Job, error) {
	j, err := s.repo.FindJob(ctx, workspaceID, jobID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Job{}, ErrJobNotFound
		}
		return Job{}, err
	}
	return j, nil
}

// ListJobs returns a workspace-scoped, keyset-paginated page of jobs.
func (s *Service) ListJobs(ctx context.Context, workspaceID uuid.UUID, req ListJobsRequest) (JobsPage, error) {
	opts := ListOpts{
		State:    req.State,
		Limit:    req.Limit,
		BeforeAt: req.BeforeAt,
		BeforeID: req.BeforeID,
	}
	jobs, err := s.repo.ListJobs(ctx, workspaceID, opts)
	if err != nil {
		return JobsPage{}, err
	}
	page := JobsPage{Jobs: jobs}
	if len(jobs) > 0 && opts.Limit > 0 && len(jobs) == opts.Limit {
		last := jobs[len(jobs)-1]
		ts := last.CreatedAt.UTC().Format(time.RFC3339Nano)
		id := last.ID.String()
		page.NextBefore = &ts
		page.NextID = &id
	}
	return page, nil
}

// ListEvents returns the audit history for a job.
func (s *Service) ListEvents(ctx context.Context, workspaceID, jobID uuid.UUID) ([]Event, error) {
	if _, err := s.GetJob(ctx, workspaceID, jobID); err != nil {
		return nil, err
	}
	return s.repo.ListEvents(ctx, jobID)
}

// ExecuteJob is the worker entry point. It loads the job, transitions
// pending→running, dispatches to the executor, and records the terminal
// transition. The function returns an error only when Asynq should retry —
// dead-letter is final and returns nil.
func (s *Service) ExecuteJob(ctx context.Context, jobID uuid.UUID) error {
	current, err := s.repo.FindJobByID(ctx, jobID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.log.Warn("execute: job missing — likely deleted", "job_id", jobID)
			return nil
		}
		return fmt.Errorf("load job: %w", err)
	}

	switch current.State {
	case StatePending, StateFailed:
		// proceed
	case StateRunning:
		s.log.Warn("execute: job already running — duplicate delivery?", "job_id", jobID)
	case StateSucceeded, StateDead, StateCancelled:
		s.log.Info("execute: job already terminal, skipping", "job_id", jobID, "state", current.State)
		return nil
	}

	now := s.clock()
	attempts := current.Attempts + 1

	runningState := StateRunning
	moved, err := s.repo.TransitionJob(ctx, current.ID, current.Version, JobPatch{
		State:     &runningState,
		Attempts:  &attempts,
		StartedAt: &now,
	})
	if err != nil {
		if errors.Is(err, ErrVersionConflict) {
			// Another worker beat us to it. Let Asynq drop / treat as success.
			s.log.Warn("execute: lost version race", "job_id", jobID)
			return nil
		}
		return fmt.Errorf("transition running: %w", err)
	}

	_ = s.repo.AppendEvent(ctx, Event{
		JobID:     moved.ID,
		Event:     "started",
		FromState: pState(current.State),
		ToState:   pState(StateRunning),
		Message:   fmt.Sprintf("attempt %d/%d", moved.Attempts, moved.MaxAttempts),
	})
	s.publish(ctx, moved.WorkspaceID, "job.started", moved)

	exec, ok := s.registry.Lookup(moved.Kind)
	if !ok {
		s.failTerminal(ctx, moved, fmt.Sprintf("unknown kind %q", moved.Kind))
		return nil // not retryable — return success to Asynq
	}

	execErr := exec(ctx, moved.Payload)
	finished := s.clock()

	if execErr == nil {
		s.complete(ctx, moved, finished)
		return nil
	}

	// Error path: decide between retry (StateFailed, Asynq will retry) and
	// terminal dead (StateDead, return nil so Asynq stops).
	errMsg := execErr.Error()
	if moved.Attempts >= moved.MaxAttempts {
		s.failTerminal(ctx, moved, errMsg)
		return nil
	}
	s.failRetry(ctx, moved, finished, errMsg)
	return execErr
}

func (s *Service) complete(ctx context.Context, j Job, at time.Time) {
	state := StateSucceeded
	updated, err := s.repo.TransitionJob(ctx, j.ID, j.Version, JobPatch{
		State:      &state,
		FinishedAt: &at,
	})
	if err != nil {
		s.log.Error("transition succeeded", "job_id", j.ID, "error", err)
		return
	}
	_ = s.repo.AppendEvent(ctx, Event{
		JobID:     updated.ID,
		Event:     "succeeded",
		FromState: pState(StateRunning),
		ToState:   pState(StateSucceeded),
	})
	s.publish(ctx, updated.WorkspaceID, "job.succeeded", updated)
	s.releaseDependents(ctx, updated)
}

func (s *Service) failRetry(ctx context.Context, j Job, at time.Time, msg string) {
	state := StateFailed
	updated, err := s.repo.TransitionJob(ctx, j.ID, j.Version, JobPatch{
		State:      &state,
		FinishedAt: &at,
		LastError:  &msg,
	})
	if err != nil {
		s.log.Error("transition failed-retry", "job_id", j.ID, "error", err)
		return
	}
	_ = s.repo.AppendEvent(ctx, Event{
		JobID:     updated.ID,
		Event:     "failed",
		FromState: pState(StateRunning),
		ToState:   pState(StateFailed),
		Message:   msg,
	})
	s.publish(ctx, updated.WorkspaceID, "job.failed", updated)
}

func (s *Service) failTerminal(ctx context.Context, j Job, msg string) {
	state := StateDead
	now := s.clock()
	updated, err := s.repo.TransitionJob(ctx, j.ID, j.Version, JobPatch{
		State:      &state,
		FinishedAt: &now,
		LastError:  &msg,
	})
	if err != nil {
		s.log.Error("transition dead", "job_id", j.ID, "error", err)
		return
	}
	_ = s.repo.AppendEvent(ctx, Event{
		JobID:     updated.ID,
		Event:     "dead",
		FromState: pState(StateRunning),
		ToState:   pState(StateDead),
		Message:   msg,
	})
	s.publish(ctx, updated.WorkspaceID, "job.dead", updated)
	s.cancelDependents(ctx, updated, "upstream job "+updated.ID.String()+" dead")
}

func pState(s State) *State { return &s }
