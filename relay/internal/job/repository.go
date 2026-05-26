package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel errors. Service layer maps these to HTTP error codes.
var (
	ErrNotFound       = errors.New("not found")
	ErrDuplicate      = errors.New("duplicate")
	ErrVersionConflict = errors.New("version conflict") // optimistic lock lost
)

// Repository is the storage contract for the job domain.
type Repository interface {
	CreatePipeline(ctx context.Context, p Pipeline) (Pipeline, error)
	FindPipelineBySlug(ctx context.Context, workspaceID uuid.UUID, slug string) (Pipeline, error)
	ListPipelines(ctx context.Context, workspaceID uuid.UUID) ([]Pipeline, error)

	CreateJob(ctx context.Context, j Job) (Job, bool, error) // bool = createdNew (false on idempotency hit)
	FindJob(ctx context.Context, workspaceID, jobID uuid.UUID) (Job, error)
	FindJobByID(ctx context.Context, jobID uuid.UUID) (Job, error)
	ListJobs(ctx context.Context, workspaceID uuid.UUID, opts ListOpts) ([]Job, error)

	TransitionJob(ctx context.Context, jobID uuid.UUID, expectedVersion int64, patch JobPatch) (Job, error)
	AppendEvent(ctx context.Context, e Event) error
	ListEvents(ctx context.Context, jobID uuid.UUID) ([]Event, error)

	ClaimReadyJobs(ctx context.Context, workspaceID uuid.UUID, limit int) ([]Job, error)

	// DAG support.
	InsertDependencies(ctx context.Context, jobID uuid.UUID, depIDs []uuid.UUID) error
	ListDependents(ctx context.Context, depJobID uuid.UUID) ([]Job, error)
	UnresolvedDeps(ctx context.Context, jobID uuid.UUID) (int, error)
}

// ListOpts drives keyset pagination on jobs.
type ListOpts struct {
	State    string
	Limit    int
	BeforeAt *time.Time
	BeforeID *uuid.UUID
}

// JobPatch is the set of fields that can change on a job transition. Pointer
// fields mean "leave unchanged when nil".
type JobPatch struct {
	State       *State
	Attempts    *int
	StartedAt   *time.Time
	FinishedAt  *time.Time
	LastError   *string
	ScheduledAt *time.Time
	BumpVersion bool // always true in practice; kept explicit for clarity
}

// PgRepository is the pgx-backed Repository.
type PgRepository struct {
	pool     *pgxpool.Pool // write/primary pool
	readPool *pgxpool.Pool // optional read replica; falls back to pool when nil
}

// RepoOption configures a PgRepository.
type RepoOption func(*PgRepository)

// WithReadPool routes read-only queries (lists, finds) through rp. Useful
// for scaling: writes hit the primary, reads hit a replica.
func WithReadPool(rp *pgxpool.Pool) RepoOption {
	return func(r *PgRepository) { r.readPool = rp }
}

// NewPgRepository builds a PgRepository. Pass WithReadPool(...) to enable
// read-replica routing on list queries.
func NewPgRepository(pool *pgxpool.Pool, opts ...RepoOption) *PgRepository {
	r := &PgRepository{pool: pool}
	for _, o := range opts {
		o(r)
	}
	return r
}

// reader returns the pool used for read-only queries — read replica if
// configured, otherwise the primary.
func (r *PgRepository) reader() *pgxpool.Pool {
	if r.readPool != nil {
		return r.readPool
	}
	return r.pool
}

func (r *PgRepository) CreatePipeline(ctx context.Context, p Pipeline) (Pipeline, error) {
	row := r.pool.QueryRow(ctx, `
		INSERT INTO pipelines (workspace_id, name, slug, description)
		VALUES ($1, $2, $3, $4)
		RETURNING id, workspace_id, name, slug, description, created_at, updated_at
	`, p.WorkspaceID, p.Name, p.Slug, p.Description)
	var out Pipeline
	if err := row.Scan(&out.ID, &out.WorkspaceID, &out.Name, &out.Slug, &out.Description, &out.CreatedAt, &out.UpdatedAt); err != nil {
		if isUniqueViolation(err) {
			return Pipeline{}, ErrDuplicate
		}
		return Pipeline{}, fmt.Errorf("insert pipeline: %w", err)
	}
	return out, nil
}

func (r *PgRepository) FindPipelineBySlug(ctx context.Context, workspaceID uuid.UUID, slug string) (Pipeline, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, workspace_id, name, slug, description, created_at, updated_at
		FROM pipelines WHERE workspace_id = $1 AND slug = $2
	`, workspaceID, slug)
	var p Pipeline
	if err := row.Scan(&p.ID, &p.WorkspaceID, &p.Name, &p.Slug, &p.Description, &p.CreatedAt, &p.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Pipeline{}, ErrNotFound
		}
		return Pipeline{}, fmt.Errorf("find pipeline: %w", err)
	}
	return p, nil
}

func (r *PgRepository) ListPipelines(ctx context.Context, workspaceID uuid.UUID) ([]Pipeline, error) {
	rows, err := r.reader().Query(ctx, `
		SELECT id, workspace_id, name, slug, description, created_at, updated_at
		FROM pipelines WHERE workspace_id = $1 ORDER BY created_at DESC
	`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	defer rows.Close()
	var out []Pipeline
	for rows.Next() {
		var p Pipeline
		if err := rows.Scan(&p.ID, &p.WorkspaceID, &p.Name, &p.Slug, &p.Description, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan pipeline: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// CreateJob inserts a job, honouring idempotency on (pipeline_id, idempotency_key).
// On idempotency hit it returns the existing job and createdNew=false.
func (r *PgRepository) CreateJob(ctx context.Context, j Job) (Job, bool, error) {
	if len(j.Payload) == 0 {
		j.Payload = json.RawMessage(`{}`)
	}

	row := r.pool.QueryRow(ctx, `
		INSERT INTO jobs (pipeline_id, workspace_id, idempotency_key, kind, payload, state, max_attempts, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (pipeline_id, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING id, pipeline_id, workspace_id, idempotency_key, kind, payload, state,
		          attempts, max_attempts, version, scheduled_at, started_at, finished_at,
		          last_error, created_at, updated_at
	`, j.PipelineID, j.WorkspaceID, j.IdempotencyKey, j.Kind, j.Payload, j.State, j.MaxAttempts, j.ScheduledAt)

	var out Job
	err := row.Scan(&out.ID, &out.PipelineID, &out.WorkspaceID, &out.IdempotencyKey, &out.Kind, &out.Payload, &out.State,
		&out.Attempts, &out.MaxAttempts, &out.Version, &out.ScheduledAt, &out.StartedAt, &out.FinishedAt,
		&out.LastError, &out.CreatedAt, &out.UpdatedAt)
	if err == nil {
		return out, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	// Idempotency hit: row exists, DO NOTHING returned no row. Fetch the
	// existing one by (pipeline_id, idempotency_key).
	if j.IdempotencyKey == nil {
		return Job{}, false, fmt.Errorf("insert job: empty result and no idempotency key")
	}
	existing, err := r.findJobByIdempotency(ctx, j.PipelineID, *j.IdempotencyKey)
	if err != nil {
		return Job{}, false, err
	}
	return existing, false, nil
}

func (r *PgRepository) findJobByIdempotency(ctx context.Context, pipelineID uuid.UUID, key string) (Job, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, pipeline_id, workspace_id, idempotency_key, kind, payload, state,
		       attempts, max_attempts, version, scheduled_at, started_at, finished_at,
		       last_error, created_at, updated_at
		FROM jobs WHERE pipeline_id = $1 AND idempotency_key = $2
	`, pipelineID, key)
	return scanJob(row)
}

func (r *PgRepository) FindJob(ctx context.Context, workspaceID, jobID uuid.UUID) (Job, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, pipeline_id, workspace_id, idempotency_key, kind, payload, state,
		       attempts, max_attempts, version, scheduled_at, started_at, finished_at,
		       last_error, created_at, updated_at
		FROM jobs WHERE workspace_id = $1 AND id = $2
	`, workspaceID, jobID)
	return scanJob(row)
}

// FindJobByID is unscoped — used by the worker which knows the id but not the
// workspace until it loads the row.
func (r *PgRepository) FindJobByID(ctx context.Context, jobID uuid.UUID) (Job, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, pipeline_id, workspace_id, idempotency_key, kind, payload, state,
		       attempts, max_attempts, version, scheduled_at, started_at, finished_at,
		       last_error, created_at, updated_at
		FROM jobs WHERE id = $1
	`, jobID)
	return scanJob(row)
}

func (r *PgRepository) ListJobs(ctx context.Context, workspaceID uuid.UUID, opts ListOpts) ([]Job, error) {
	if opts.Limit <= 0 || opts.Limit > 200 {
		opts.Limit = 50
	}

	q := `
		SELECT id, pipeline_id, workspace_id, idempotency_key, kind, payload, state,
		       attempts, max_attempts, version, scheduled_at, started_at, finished_at,
		       last_error, created_at, updated_at
		FROM jobs
		WHERE workspace_id = $1
	`
	args := []any{workspaceID}
	if opts.State != "" {
		args = append(args, opts.State)
		q += fmt.Sprintf(" AND state = $%d", len(args))
	}
	if opts.BeforeAt != nil && opts.BeforeID != nil {
		args = append(args, *opts.BeforeAt, *opts.BeforeID)
		q += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, opts.Limit)
	q += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))

	rows, err := r.reader().Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJobRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// TransitionJob applies a patch to a job with optimistic version locking.
// Returns ErrVersionConflict if expectedVersion != current version.
func (r *PgRepository) TransitionJob(ctx context.Context, jobID uuid.UUID, expectedVersion int64, patch JobPatch) (Job, error) {
	q := `UPDATE jobs SET version = version + 1, updated_at = NOW()`
	args := []any{}
	add := func(col string, val any) {
		args = append(args, val)
		q += fmt.Sprintf(", %s = $%d", col, len(args))
	}
	if patch.State != nil {
		add("state", string(*patch.State))
	}
	if patch.Attempts != nil {
		add("attempts", *patch.Attempts)
	}
	if patch.StartedAt != nil {
		add("started_at", *patch.StartedAt)
	}
	if patch.FinishedAt != nil {
		add("finished_at", *patch.FinishedAt)
	}
	if patch.LastError != nil {
		add("last_error", *patch.LastError)
	}
	if patch.ScheduledAt != nil {
		add("scheduled_at", *patch.ScheduledAt)
	}
	args = append(args, jobID, expectedVersion)
	q += fmt.Sprintf(`
		WHERE id = $%d AND version = $%d
		RETURNING id, pipeline_id, workspace_id, idempotency_key, kind, payload, state,
		          attempts, max_attempts, version, scheduled_at, started_at, finished_at,
		          last_error, created_at, updated_at
	`, len(args)-1, len(args))

	row := r.pool.QueryRow(ctx, q, args...)
	out, err := scanJob(row)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Job{}, ErrVersionConflict
		}
		return Job{}, err
	}
	return out, nil
}

func (r *PgRepository) AppendEvent(ctx context.Context, e Event) error {
	if len(e.Metadata) == 0 {
		e.Metadata = json.RawMessage(`{}`)
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO job_events (job_id, event, from_state, to_state, message, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, e.JobID, e.Event, nullableState(e.FromState), nullableState(e.ToState), e.Message, e.Metadata)
	if err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	return nil
}

func (r *PgRepository) ListEvents(ctx context.Context, jobID uuid.UUID) ([]Event, error) {
	rows, err := r.reader().Query(ctx, `
		SELECT id, job_id, event, from_state, to_state, message, metadata, created_at
		FROM job_events WHERE job_id = $1 ORDER BY id ASC
	`, jobID)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var ev Event
		var from, to *string
		if err := rows.Scan(&ev.ID, &ev.JobID, &ev.Event, &from, &to, &ev.Message, &ev.Metadata, &ev.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		ev.FromState = stateFromString(from)
		ev.ToState = stateFromString(to)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// ClaimReadyJobs grabs up to limit pending+due jobs with FOR UPDATE SKIP LOCKED.
// Used by recovery sweeps that re-enqueue jobs whose Asynq task was lost.
func (r *PgRepository) ClaimReadyJobs(ctx context.Context, workspaceID uuid.UUID, limit int) ([]Job, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id, pipeline_id, workspace_id, idempotency_key, kind, payload, state,
		       attempts, max_attempts, version, scheduled_at, started_at, finished_at,
		       last_error, created_at, updated_at
		FROM jobs
		WHERE workspace_id = $1
		  AND state = 'pending'
		  AND scheduled_at <= NOW()
		ORDER BY scheduled_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, workspaceID, limit)
	if err != nil {
		return nil, fmt.Errorf("claim ready jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJobRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// rowScanner unifies QueryRow.Scan and Rows.Scan for scanJob*.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(s rowScanner) (Job, error) {
	var j Job
	err := s.Scan(&j.ID, &j.PipelineID, &j.WorkspaceID, &j.IdempotencyKey, &j.Kind, &j.Payload, &j.State,
		&j.Attempts, &j.MaxAttempts, &j.Version, &j.ScheduledAt, &j.StartedAt, &j.FinishedAt,
		&j.LastError, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrNotFound
		}
		return Job{}, fmt.Errorf("scan job: %w", err)
	}
	return j, nil
}

func scanJobRow(rows pgx.Rows) (Job, error) {
	var j Job
	err := rows.Scan(&j.ID, &j.PipelineID, &j.WorkspaceID, &j.IdempotencyKey, &j.Kind, &j.Payload, &j.State,
		&j.Attempts, &j.MaxAttempts, &j.Version, &j.ScheduledAt, &j.StartedAt, &j.FinishedAt,
		&j.LastError, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return Job{}, fmt.Errorf("scan job: %w", err)
	}
	return j, nil
}

func nullableState(s *State) any {
	if s == nil {
		return nil
	}
	return string(*s)
}

func stateFromString(s *string) *State {
	if s == nil {
		return nil
	}
	st := State(*s)
	return &st
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// InsertDependencies inserts (jobID -> depID) edges for every dep in depIDs.
// Duplicate edges are silently ignored. The transaction ensures partial
// insertions don't leak.
func (r *PgRepository) InsertDependencies(ctx context.Context, jobID uuid.UUID, depIDs []uuid.UUID) error {
	if len(depIDs) == 0 {
		return nil
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin deps tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	for _, dep := range depIDs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO job_dependencies (job_id, depends_on_job_id)
			VALUES ($1, $2) ON CONFLICT DO NOTHING
		`, jobID, dep); err != nil {
			return fmt.Errorf("insert dep: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit deps: %w", err)
	}
	return nil
}

// ListDependents returns every job that has depJobID as a dependency. Used
// to wake blocked jobs after a dependency reaches a terminal state.
func (r *PgRepository) ListDependents(ctx context.Context, depJobID uuid.UUID) ([]Job, error) {
	rows, err := r.reader().Query(ctx, `
		SELECT j.id, j.pipeline_id, j.workspace_id, j.idempotency_key, j.kind, j.payload, j.state,
		       j.attempts, j.max_attempts, j.version, j.scheduled_at, j.started_at, j.finished_at,
		       j.last_error, j.created_at, j.updated_at
		FROM jobs j
		JOIN job_dependencies d ON d.job_id = j.id
		WHERE d.depends_on_job_id = $1
	`, depJobID)
	if err != nil {
		return nil, fmt.Errorf("list dependents: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJobRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// UnresolvedDeps returns the count of dependencies for jobID that are not yet
// in 'succeeded' state. Zero means the job is ready to run.
func (r *PgRepository) UnresolvedDeps(ctx context.Context, jobID uuid.UUID) (int, error) {
	var n int
	err := r.reader().QueryRow(ctx, `
		SELECT COUNT(*) FROM job_dependencies d
		JOIN jobs dep ON dep.id = d.depends_on_job_id
		WHERE d.job_id = $1 AND dep.state <> 'succeeded'
	`, jobID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count unresolved deps: %w", err)
	}
	return n, nil
}
