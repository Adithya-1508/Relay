package job

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// mockRepo is an in-memory Repository for unit tests.
type mockRepo struct {
	mu         sync.Mutex
	pipelines  map[uuid.UUID]Pipeline
	pipeBySlug map[string]uuid.UUID
	jobs       map[uuid.UUID]Job
	jobsByKey  map[string]uuid.UUID // pipelineID|idem -> jobID
	events     []Event
	deps       map[uuid.UUID][]uuid.UUID // jobID -> []depJobID
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		pipelines:  map[uuid.UUID]Pipeline{},
		pipeBySlug: map[string]uuid.UUID{},
		jobs:       map[uuid.UUID]Job{},
		jobsByKey:  map[string]uuid.UUID{},
		deps:       map[uuid.UUID][]uuid.UUID{},
	}
}

func (m *mockRepo) CreatePipeline(_ context.Context, p Pipeline) (Pipeline, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := p.WorkspaceID.String() + "|" + p.Slug
	if _, exists := m.pipeBySlug[key]; exists {
		return Pipeline{}, ErrDuplicate
	}
	p.ID = uuid.New()
	now := time.Now()
	p.CreatedAt, p.UpdatedAt = now, now
	m.pipelines[p.ID] = p
	m.pipeBySlug[key] = p.ID
	return p, nil
}

func (m *mockRepo) FindPipelineBySlug(_ context.Context, workspaceID uuid.UUID, slug string) (Pipeline, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.pipeBySlug[workspaceID.String()+"|"+slug]
	if !ok {
		return Pipeline{}, ErrNotFound
	}
	return m.pipelines[id], nil
}

func (m *mockRepo) ListPipelines(_ context.Context, workspaceID uuid.UUID) ([]Pipeline, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Pipeline
	for _, p := range m.pipelines {
		if p.WorkspaceID == workspaceID {
			out = append(out, p)
		}
	}
	return out, nil
}

func (m *mockRepo) CreateJob(_ context.Context, j Job) (Job, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j.IdempotencyKey != nil {
		k := j.PipelineID.String() + "|" + *j.IdempotencyKey
		if existing, ok := m.jobsByKey[k]; ok {
			return m.jobs[existing], false, nil
		}
	}
	j.ID = uuid.New()
	if j.Version == 0 {
		j.Version = 1
	}
	now := time.Now()
	j.CreatedAt, j.UpdatedAt = now, now
	if j.ScheduledAt.IsZero() {
		j.ScheduledAt = now
	}
	if len(j.Payload) == 0 {
		j.Payload = json.RawMessage(`{}`)
	}
	m.jobs[j.ID] = j
	if j.IdempotencyKey != nil {
		m.jobsByKey[j.PipelineID.String()+"|"+*j.IdempotencyKey] = j.ID
	}
	return j, true, nil
}

func (m *mockRepo) FindJob(_ context.Context, workspaceID, jobID uuid.UUID) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok || j.WorkspaceID != workspaceID {
		return Job{}, ErrNotFound
	}
	return j, nil
}

func (m *mockRepo) FindJobByID(_ context.Context, jobID uuid.UUID) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return Job{}, ErrNotFound
	}
	return j, nil
}

func (m *mockRepo) ListJobs(_ context.Context, workspaceID uuid.UUID, opts ListOpts) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Job
	for _, j := range m.jobs {
		if j.WorkspaceID != workspaceID {
			continue
		}
		if opts.State != "" && string(j.State) != opts.State {
			continue
		}
		out = append(out, j)
	}
	return out, nil
}

func (m *mockRepo) TransitionJob(_ context.Context, jobID uuid.UUID, expectedVersion int64, patch JobPatch) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return Job{}, ErrNotFound
	}
	if j.Version != expectedVersion {
		return Job{}, ErrVersionConflict
	}
	if patch.State != nil {
		j.State = *patch.State
	}
	if patch.Attempts != nil {
		j.Attempts = *patch.Attempts
	}
	if patch.StartedAt != nil {
		j.StartedAt = patch.StartedAt
	}
	if patch.FinishedAt != nil {
		j.FinishedAt = patch.FinishedAt
	}
	if patch.LastError != nil {
		j.LastError = patch.LastError
	}
	if patch.ScheduledAt != nil {
		j.ScheduledAt = *patch.ScheduledAt
	}
	j.Version++
	j.UpdatedAt = time.Now()
	m.jobs[jobID] = j
	return j, nil
}

func (m *mockRepo) AppendEvent(_ context.Context, e Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e.ID = int64(len(m.events) + 1)
	e.CreatedAt = time.Now()
	m.events = append(m.events, e)
	return nil
}

func (m *mockRepo) ListEvents(_ context.Context, jobID uuid.UUID) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Event
	for _, e := range m.events {
		if e.JobID == jobID {
			out = append(out, e)
		}
	}
	return out, nil
}

func (m *mockRepo) ClaimReadyJobs(_ context.Context, workspaceID uuid.UUID, limit int) ([]Job, error) {
	return nil, nil // unused in tests
}

func (m *mockRepo) InsertDependencies(_ context.Context, jobID uuid.UUID, depIDs []uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deps[jobID] = append(m.deps[jobID], depIDs...)
	return nil
}

func (m *mockRepo) ListDependents(_ context.Context, depJobID uuid.UUID) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Job
	for jobID, deps := range m.deps {
		for _, d := range deps {
			if d == depJobID {
				out = append(out, m.jobs[jobID])
				break
			}
		}
	}
	return out, nil
}

func (m *mockRepo) UnresolvedDeps(_ context.Context, jobID uuid.UUID) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, depID := range m.deps[jobID] {
		dep, ok := m.jobs[depID]
		if !ok || dep.State != StateSucceeded {
			n++
		}
	}
	return n, nil
}

// fakeEnqueuer records calls without touching Redis.
type fakeEnqueuer struct {
	mu      sync.Mutex
	enqueues []uuid.UUID
}

func (f *fakeEnqueuer) Enqueue(_ context.Context, id uuid.UUID, _ EnqueueOpts) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueues = append(f.enqueues, id)
	return nil
}
func (f *fakeEnqueuer) Close() error { return nil }

func newTestService(t *testing.T) (*Service, *mockRepo, *fakeEnqueuer) {
	t.Helper()
	repo := newMockRepo()
	enq := &fakeEnqueuer{}
	reg := NewRegistry()
	reg.RegisterBuiltins()
	svc := NewService(Config{
		Repository:         repo,
		Enqueuer:           enq,
		Registry:           reg,
		DefaultMaxAttempts: 3,
	})
	return svc, repo, enq
}

func TestCreatePipelineDuplicate(t *testing.T) {
	svc, _, _ := newTestService(t)
	ws := uuid.New()
	if _, err := svc.CreatePipeline(context.Background(), ws, CreatePipelineRequest{
		Name: "Daily ETL", Slug: "daily-etl",
	}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err := svc.CreatePipeline(context.Background(), ws, CreatePipelineRequest{
		Name: "Daily ETL", Slug: "daily-etl",
	}); !errors.Is(err, ErrPipelineExists) {
		t.Fatalf("expected ErrPipelineExists, got %v", err)
	}
}

func TestEnqueueJobIdempotent(t *testing.T) {
	svc, _, enq := newTestService(t)
	ws := uuid.New()
	ctx := context.Background()
	if _, err := svc.CreatePipeline(ctx, ws, CreatePipelineRequest{Name: "P", Slug: "p"}); err != nil {
		t.Fatal(err)
	}
	key := "dedup-key-1"
	req := EnqueueJobRequest{
		Kind:           "noop",
		IdempotencyKey: &key,
	}
	first, err := svc.EnqueueJob(ctx, ws, "p", req)
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	second, err := svc.EnqueueJob(ctx, ws, "p", req)
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("idempotency hit returned different id: %v vs %v", first.ID, second.ID)
	}
	if len(enq.enqueues) != 1 {
		t.Fatalf("expected exactly 1 asynq enqueue (dedup), got %d", len(enq.enqueues))
	}
}

func TestEnqueueUnknownKind(t *testing.T) {
	svc, _, _ := newTestService(t)
	ws := uuid.New()
	ctx := context.Background()
	if _, err := svc.CreatePipeline(ctx, ws, CreatePipelineRequest{Name: "P", Slug: "p"}); err != nil {
		t.Fatal(err)
	}
	_, err := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{Kind: "nope"})
	if !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("expected ErrUnknownKind, got %v", err)
	}
}

func TestExecuteSuccessfulNoop(t *testing.T) {
	svc, repo, _ := newTestService(t)
	ws := uuid.New()
	ctx := context.Background()
	if _, err := svc.CreatePipeline(ctx, ws, CreatePipelineRequest{Name: "P", Slug: "p"}); err != nil {
		t.Fatal(err)
	}
	j, err := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{Kind: "noop"})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ExecuteJob(ctx, j.ID); err != nil {
		t.Fatalf("execute: %v", err)
	}
	got, err := repo.FindJobByID(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateSucceeded {
		t.Fatalf("expected succeeded, got %s", got.State)
	}
	if got.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", got.Attempts)
	}
}

// failingExecutor lets us test the retry/dead paths.
func registerFailing(reg *Registry, attempts int) {
	calls := 0
	reg.Register("flaky", func(_ context.Context, _ json.RawMessage) error {
		calls++
		if calls >= attempts {
			return nil
		}
		return errors.New("transient")
	})
	reg.Register("always-fails", func(_ context.Context, _ json.RawMessage) error {
		return errors.New("permanent")
	})
}

func TestExecuteRetryThenDead(t *testing.T) {
	svc, repo, _ := newTestService(t)
	registerFailing(svc.registry, 999) // always-fails
	ws := uuid.New()
	ctx := context.Background()
	if _, err := svc.CreatePipeline(ctx, ws, CreatePipelineRequest{Name: "P", Slug: "p"}); err != nil {
		t.Fatal(err)
	}
	maxAttempts := 2
	j, err := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{
		Kind: "always-fails", MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}

	// First attempt — should fail with retry path (state failed, error returned).
	if err := svc.ExecuteJob(ctx, j.ID); err == nil {
		t.Fatal("expected first attempt to return error so Asynq retries")
	}
	mid, err := repo.FindJobByID(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if mid.State != StateFailed {
		t.Fatalf("expected failed, got %s", mid.State)
	}

	// Second attempt — attempts == max, terminal dead. Service returns nil so
	// Asynq stops retrying.
	if err := svc.ExecuteJob(ctx, j.ID); err != nil {
		t.Fatalf("expected nil on terminal dead, got %v", err)
	}
	final, err := repo.FindJobByID(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != StateDead {
		t.Fatalf("expected dead, got %s", final.State)
	}
	if final.Attempts != maxAttempts {
		t.Fatalf("expected attempts=%d, got %d", maxAttempts, final.Attempts)
	}
}

func TestDAGLinearRelease(t *testing.T) {
	svc, repo, enq := newTestService(t)
	ws := uuid.New()
	ctx := context.Background()
	if _, err := svc.CreatePipeline(ctx, ws, CreatePipelineRequest{Name: "P", Slug: "p"}); err != nil {
		t.Fatal(err)
	}

	a, err := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{Kind: "noop"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{Kind: "noop", DependsOn: []uuid.UUID{a.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if b.State != StateBlocked {
		t.Fatalf("expected b blocked, got %s", b.State)
	}
	// Asynq should have one task (for a only), not two.
	if len(enq.enqueues) != 1 {
		t.Fatalf("expected 1 enqueue (a only), got %d", len(enq.enqueues))
	}

	// Run a to completion.
	if err := svc.ExecuteJob(ctx, a.ID); err != nil {
		t.Fatalf("execute a: %v", err)
	}

	// b should now have been released + enqueued.
	got, err := repo.FindJobByID(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StatePending {
		t.Fatalf("expected b pending after release, got %s", got.State)
	}
	if len(enq.enqueues) != 2 {
		t.Fatalf("expected 2 enqueues after release, got %d", len(enq.enqueues))
	}
}

func TestDAGFanOutWaitsForAllDeps(t *testing.T) {
	svc, repo, _ := newTestService(t)
	ws := uuid.New()
	ctx := context.Background()
	if _, err := svc.CreatePipeline(ctx, ws, CreatePipelineRequest{Name: "P", Slug: "p"}); err != nil {
		t.Fatal(err)
	}

	a, _ := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{Kind: "noop"})
	b, _ := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{Kind: "noop"})
	c, _ := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{
		Kind:      "noop",
		DependsOn: []uuid.UUID{a.ID, b.ID},
	})

	// Complete only a; c must still be blocked.
	if err := svc.ExecuteJob(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.FindJobByID(ctx, c.ID)
	if got.State != StateBlocked {
		t.Fatalf("expected c still blocked after only 1/2 deps done, got %s", got.State)
	}

	// Complete b; c should now flip to pending.
	if err := svc.ExecuteJob(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = repo.FindJobByID(ctx, c.ID)
	if got.State != StatePending {
		t.Fatalf("expected c pending after both deps done, got %s", got.State)
	}
}

func TestDAGUpstreamDeadCancelsBlocked(t *testing.T) {
	svc, repo, _ := newTestService(t)
	registerFailing(svc.registry, 999)
	ws := uuid.New()
	ctx := context.Background()
	if _, err := svc.CreatePipeline(ctx, ws, CreatePipelineRequest{Name: "P", Slug: "p"}); err != nil {
		t.Fatal(err)
	}

	a, _ := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{Kind: "always-fails", MaxAttempts: 1})
	b, _ := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{Kind: "noop", DependsOn: []uuid.UUID{a.ID}})

	// One attempt → terminal dead (MaxAttempts: 1).
	_ = svc.ExecuteJob(ctx, a.ID)
	deadA, _ := repo.FindJobByID(ctx, a.ID)
	if deadA.State != StateDead {
		t.Fatalf("expected a dead, got %s", deadA.State)
	}
	cancelledB, _ := repo.FindJobByID(ctx, b.ID)
	if cancelledB.State != StateCancelled {
		t.Fatalf("expected b cancelled by upstream dead, got %s", cancelledB.State)
	}
}

func TestExecuteVersionConflictNoCrash(t *testing.T) {
	svc, repo, _ := newTestService(t)
	ws := uuid.New()
	ctx := context.Background()
	if _, err := svc.CreatePipeline(ctx, ws, CreatePipelineRequest{Name: "P", Slug: "p"}); err != nil {
		t.Fatal(err)
	}
	j, err := svc.EnqueueJob(ctx, ws, "p", EnqueueJobRequest{Kind: "noop"})
	if err != nil {
		t.Fatal(err)
	}
	// Bump version externally to simulate concurrent transition.
	stolen := StateCancelled
	if _, err := repo.TransitionJob(ctx, j.ID, j.Version, JobPatch{State: &stolen}); err != nil {
		t.Fatalf("setup transition: %v", err)
	}
	// Now Execute should observe terminal state and skip.
	if err := svc.ExecuteJob(ctx, j.ID); err != nil {
		t.Fatalf("expected nil (already terminal), got %v", err)
	}
}
