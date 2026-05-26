package auth

import (
	"time"

	"github.com/google/uuid"
)

// Role enumerates workspace-scoped permissions. Matches the CHECK constraint
// in 000001_init.up.sql.
type Role string

const (
	RoleOwner  Role = "owner"
	RoleAdmin  Role = "admin"
	RoleMember Role = "member"
)

// Workspace mirrors the workspaces table.
type Workspace struct {
	ID        uuid.UUID  `json:"id"`
	Name      string     `json:"name"`
	Slug      string     `json:"slug"`
	OwnerID   *uuid.UUID `json:"owner_id,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// User mirrors the users table. PasswordHash is tagged json:"-" so it is
// compiler-impossible to leak through any handler that serialises a User.
type User struct {
	ID           uuid.UUID `json:"id"`
	WorkspaceID  uuid.UUID `json:"workspace_id"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	Role         Role      `json:"role"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// RefreshToken mirrors the refresh_tokens table. TokenHash never leaves this
// process unhashed; the raw token only exists in memory during issue.
type RefreshToken struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	FamilyID  uuid.UUID
	TokenHash string
	UserAgent *string
	IPAddress *string
	ExpiresAt time.Time
	RevokedAt *time.Time
	CreatedAt time.Time
}

// Revoked is true when the token has been retired (either by rotation or
// explicit logout).
func (t *RefreshToken) Revoked() bool {
	return t.RevokedAt != nil
}

// Expired is true at or after the token's expires_at.
func (t *RefreshToken) Expired(now time.Time) bool {
	return !now.Before(t.ExpiresAt)
}
