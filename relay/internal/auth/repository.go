package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel errors returned by the repository. Service layer maps these to
// HTTP error codes.
var (
	ErrNotFound  = errors.New("not found")
	ErrDuplicate = errors.New("duplicate")
)

// Repository is the storage contract for the auth domain. Defined as an
// interface so the service layer can be unit-tested with a mock.
type Repository interface {
	CreateWorkspaceAndOwner(ctx context.Context, ws Workspace, u User) (Workspace, User, error)
	FindUserByEmail(ctx context.Context, email string) (User, error)
	FindUserByID(ctx context.Context, id uuid.UUID) (User, error)
	FindWorkspaceByID(ctx context.Context, id uuid.UUID) (Workspace, error)

	InsertRefreshToken(ctx context.Context, t RefreshToken) error
	FindRefreshTokenByHash(ctx context.Context, hash string) (RefreshToken, error)
	RevokeRefreshToken(ctx context.Context, id uuid.UUID, now time.Time) error
	RevokeFamily(ctx context.Context, familyID uuid.UUID, now time.Time) error
}

// PgRepository is the pgx-backed Repository implementation.
type PgRepository struct {
	pool *pgxpool.Pool
}

// NewPgRepository builds a PgRepository.
func NewPgRepository(pool *pgxpool.Pool) *PgRepository {
	return &PgRepository{pool: pool}
}

// CreateWorkspaceAndOwner performs the two-step workspace/user insert atomically:
//  1. INSERT workspace with owner_id NULL
//  2. INSERT user with workspace_id pointing at the new workspace
//  3. UPDATE workspace.owner_id to the new user
//
// Wrapped in a transaction so a crash between steps leaves no orphan rows.
// Returns ErrDuplicate if the email is already taken.
func (r *PgRepository) CreateWorkspaceAndOwner(ctx context.Context, ws Workspace, u User) (Workspace, User, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Workspace{}, User{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // best-effort on failure path

	wsRow := tx.QueryRow(ctx, `
		INSERT INTO workspaces (name, slug)
		VALUES ($1, $2)
		RETURNING id, name, slug, owner_id, created_at, updated_at
	`, ws.Name, ws.Slug)
	var inserted Workspace
	if err := wsRow.Scan(&inserted.ID, &inserted.Name, &inserted.Slug, &inserted.OwnerID, &inserted.CreatedAt, &inserted.UpdatedAt); err != nil {
		if isUniqueViolation(err) {
			return Workspace{}, User{}, ErrDuplicate
		}
		return Workspace{}, User{}, fmt.Errorf("insert workspace: %w", err)
	}

	userRow := tx.QueryRow(ctx, `
		INSERT INTO users (workspace_id, email, password_hash, role)
		VALUES ($1, $2, $3, $4)
		RETURNING id, workspace_id, email, password_hash, role, created_at, updated_at
	`, inserted.ID, u.Email, u.PasswordHash, string(RoleOwner))
	var insertedUser User
	if err := userRow.Scan(&insertedUser.ID, &insertedUser.WorkspaceID, &insertedUser.Email, &insertedUser.PasswordHash, &insertedUser.Role, &insertedUser.CreatedAt, &insertedUser.UpdatedAt); err != nil {
		if isUniqueViolation(err) {
			return Workspace{}, User{}, ErrDuplicate
		}
		return Workspace{}, User{}, fmt.Errorf("insert user: %w", err)
	}

	if _, err := tx.Exec(ctx, `UPDATE workspaces SET owner_id = $1, updated_at = NOW() WHERE id = $2`, insertedUser.ID, inserted.ID); err != nil {
		return Workspace{}, User{}, fmt.Errorf("link owner: %w", err)
	}
	inserted.OwnerID = &insertedUser.ID

	if err := tx.Commit(ctx); err != nil {
		return Workspace{}, User{}, fmt.Errorf("commit: %w", err)
	}
	return inserted, insertedUser, nil
}

// FindUserByEmail looks up a user case-insensitively (email column is citext).
func (r *PgRepository) FindUserByEmail(ctx context.Context, email string) (User, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, workspace_id, email, password_hash, role, created_at, updated_at
		FROM users WHERE email = $1
	`, email)
	var u User
	if err := row.Scan(&u.ID, &u.WorkspaceID, &u.Email, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, fmt.Errorf("find user by email: %w", err)
	}
	return u, nil
}

// FindUserByID retrieves a user by primary key.
func (r *PgRepository) FindUserByID(ctx context.Context, id uuid.UUID) (User, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, workspace_id, email, password_hash, role, created_at, updated_at
		FROM users WHERE id = $1
	`, id)
	var u User
	if err := row.Scan(&u.ID, &u.WorkspaceID, &u.Email, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, fmt.Errorf("find user by id: %w", err)
	}
	return u, nil
}

// FindWorkspaceByID retrieves a workspace by primary key.
func (r *PgRepository) FindWorkspaceByID(ctx context.Context, id uuid.UUID) (Workspace, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, name, slug, owner_id, created_at, updated_at
		FROM workspaces WHERE id = $1
	`, id)
	var w Workspace
	if err := row.Scan(&w.ID, &w.Name, &w.Slug, &w.OwnerID, &w.CreatedAt, &w.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Workspace{}, ErrNotFound
		}
		return Workspace{}, fmt.Errorf("find workspace: %w", err)
	}
	return w, nil
}

// InsertRefreshToken persists a freshly-issued refresh token row.
func (r *PgRepository) InsertRefreshToken(ctx context.Context, t RefreshToken) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO refresh_tokens (id, user_id, family_id, token_hash, user_agent, ip_address, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, t.ID, t.UserID, t.FamilyID, t.TokenHash, t.UserAgent, t.IPAddress, t.ExpiresAt, t.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert refresh token: %w", err)
	}
	return nil
}

// FindRefreshTokenByHash looks up a refresh token by its SHA-256 hash.
func (r *PgRepository) FindRefreshTokenByHash(ctx context.Context, hash string) (RefreshToken, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, user_id, family_id, token_hash, user_agent, ip_address, expires_at, revoked_at, created_at
		FROM refresh_tokens WHERE token_hash = $1
	`, hash)
	var t RefreshToken
	if err := row.Scan(&t.ID, &t.UserID, &t.FamilyID, &t.TokenHash, &t.UserAgent, &t.IPAddress, &t.ExpiresAt, &t.RevokedAt, &t.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RefreshToken{}, ErrNotFound
		}
		return RefreshToken{}, fmt.Errorf("find refresh token: %w", err)
	}
	return t, nil
}

// RevokeRefreshToken marks a single token revoked.
func (r *PgRepository) RevokeRefreshToken(ctx context.Context, id uuid.UUID, now time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE refresh_tokens SET revoked_at = $1 WHERE id = $2 AND revoked_at IS NULL
	`, now, id)
	if err != nil {
		return fmt.Errorf("revoke token: %w", err)
	}
	return nil
}

// RevokeFamily marks every token in a family revoked. Called when theft is
// detected (a revoked token from the family is re-used).
func (r *PgRepository) RevokeFamily(ctx context.Context, familyID uuid.UUID, now time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE refresh_tokens SET revoked_at = $1 WHERE family_id = $2 AND revoked_at IS NULL
	`, now, familyID)
	if err != nil {
		return fmt.Errorf("revoke family: %w", err)
	}
	return nil
}

// isUniqueViolation reports whether err is a Postgres unique-constraint
// violation (sqlstate 23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
