package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adithya/relay/pkg/config"
)

// mockRepo is an in-memory Repository for unit tests. Indexed by primary
// fields so each lookup is straightforward.
type mockRepo struct {
	users      map[uuid.UUID]User
	workspaces map[uuid.UUID]Workspace
	usersByEm  map[string]uuid.UUID
	tokens     map[string]RefreshToken // by hash
	tokensByID map[uuid.UUID]string    // id -> hash, lets us update revoke
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		users:      map[uuid.UUID]User{},
		workspaces: map[uuid.UUID]Workspace{},
		usersByEm:  map[string]uuid.UUID{},
		tokens:     map[string]RefreshToken{},
		tokensByID: map[uuid.UUID]string{},
	}
}

func (m *mockRepo) CreateWorkspaceAndOwner(_ context.Context, ws Workspace, u User) (Workspace, User, error) {
	if _, exists := m.usersByEm[u.Email]; exists {
		return Workspace{}, User{}, ErrDuplicate
	}
	ws.ID = uuid.New()
	now := time.Now()
	ws.CreatedAt, ws.UpdatedAt = now, now

	u.ID = uuid.New()
	u.WorkspaceID = ws.ID
	u.Role = RoleOwner
	u.CreatedAt, u.UpdatedAt = now, now

	ws.OwnerID = &u.ID

	m.workspaces[ws.ID] = ws
	m.users[u.ID] = u
	m.usersByEm[u.Email] = u.ID
	return ws, u, nil
}

func (m *mockRepo) FindUserByEmail(_ context.Context, email string) (User, error) {
	id, ok := m.usersByEm[email]
	if !ok {
		return User{}, ErrNotFound
	}
	return m.users[id], nil
}

func (m *mockRepo) FindUserByID(_ context.Context, id uuid.UUID) (User, error) {
	u, ok := m.users[id]
	if !ok {
		return User{}, ErrNotFound
	}
	return u, nil
}

func (m *mockRepo) FindWorkspaceByID(_ context.Context, id uuid.UUID) (Workspace, error) {
	w, ok := m.workspaces[id]
	if !ok {
		return Workspace{}, ErrNotFound
	}
	return w, nil
}

func (m *mockRepo) InsertRefreshToken(_ context.Context, t RefreshToken) error {
	m.tokens[t.TokenHash] = t
	m.tokensByID[t.ID] = t.TokenHash
	return nil
}

func (m *mockRepo) FindRefreshTokenByHash(_ context.Context, hash string) (RefreshToken, error) {
	t, ok := m.tokens[hash]
	if !ok {
		return RefreshToken{}, ErrNotFound
	}
	return t, nil
}

func (m *mockRepo) RevokeRefreshToken(_ context.Context, id uuid.UUID, now time.Time) error {
	hash, ok := m.tokensByID[id]
	if !ok {
		return nil
	}
	t := m.tokens[hash]
	t.RevokedAt = &now
	m.tokens[hash] = t
	return nil
}

func (m *mockRepo) RevokeFamily(_ context.Context, familyID uuid.UUID, now time.Time) error {
	for h, t := range m.tokens {
		if t.FamilyID == familyID && t.RevokedAt == nil {
			t.RevokedAt = &now
			m.tokens[h] = t
		}
	}
	return nil
}

func newTestService(t *testing.T) (*Service, *mockRepo) {
	t.Helper()
	repo := newMockRepo()
	svc := NewService(repo, config.JWTConfig{
		AccessSecret:  "test-access-secret-min-32-chars",
		RefreshSecret: "test-refresh-secret-min-32-chars",
		AccessExpiry:  15 * time.Minute,
		RefreshExpiry: 24 * time.Hour,
	}, time.Now)
	return svc, repo
}

func TestRegisterAndLogin(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	regResp, err := svc.Register(ctx, RegisterRequest{
		Email:         "alice@example.com",
		Password:      "supersecret",
		WorkspaceName: "Acme",
		WorkspaceSlug: "acme",
	}, "test-ua", "127.0.0.1")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if regResp.User.PasswordHash != "" {
		// json:"-" handles serialisation; the in-process struct still has the hash
		// for callers, which is fine. We just want to confirm a hash was set.
	}
	if regResp.Tokens.AccessToken == "" || regResp.Tokens.RefreshToken == "" {
		t.Fatal("expected tokens to be issued")
	}

	loginResp, err := svc.Login(ctx, LoginRequest{
		Email:    "alice@example.com",
		Password: "supersecret",
	}, "test-ua", "127.0.0.1")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if loginResp.User.ID != regResp.User.ID {
		t.Fatalf("login returned different user: %v vs %v", loginResp.User.ID, regResp.User.ID)
	}

	claims, err := svc.VerifyAccessToken(loginResp.Tokens.AccessToken)
	if err != nil {
		t.Fatalf("verify access token: %v", err)
	}
	if claims.UserID != loginResp.User.ID {
		t.Fatalf("claim user id mismatch: %v vs %v", claims.UserID, loginResp.User.ID)
	}
	if claims.Role != RoleOwner {
		t.Fatalf("expected owner role, got %s", claims.Role)
	}
}

func TestLoginWrongPassword(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	if _, err := svc.Register(ctx, RegisterRequest{
		Email: "bob@example.com", Password: "rightpass",
		WorkspaceName: "Bob", WorkspaceSlug: "bob",
	}, "", ""); err != nil {
		t.Fatalf("register: %v", err)
	}

	_, err := svc.Login(ctx, LoginRequest{Email: "bob@example.com", Password: "wrongpass"}, "", "")
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials, got %v", err)
	}

	// Unknown email returns the same sentinel — user enumeration prevention.
	_, err = svc.Login(ctx, LoginRequest{Email: "ghost@example.com", Password: "whatever"}, "", "")
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials for unknown email, got %v", err)
	}
}

func TestRefreshRotationAndTheftDetection(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	reg, err := svc.Register(ctx, RegisterRequest{
		Email: "carol@example.com", Password: "supersecret",
		WorkspaceName: "Carol", WorkspaceSlug: "carol",
	}, "", "")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	first := reg.Tokens.RefreshToken
	rotated, err := svc.Refresh(ctx, first, "", "")
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if rotated.Tokens.RefreshToken == first {
		t.Fatal("refresh did not rotate the refresh token")
	}

	// Replaying the original refresh token must trigger ErrTokenRevoked AND
	// kill the family — so the rotated one becomes invalid too.
	if _, err := svc.Refresh(ctx, first, "", ""); !errors.Is(err, ErrTokenRevoked) {
		t.Fatalf("expected ErrTokenRevoked on replay, got %v", err)
	}
	if _, err := svc.Refresh(ctx, rotated.Tokens.RefreshToken, "", ""); !errors.Is(err, ErrTokenRevoked) {
		t.Fatalf("expected family-kill on rotated token after theft, got %v", err)
	}
}

func TestDuplicateEmail(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	req := RegisterRequest{
		Email: "dave@example.com", Password: "supersecret",
		WorkspaceName: "Dave", WorkspaceSlug: "dave",
	}
	if _, err := svc.Register(ctx, req, "", ""); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if _, err := svc.Register(ctx, req, "", ""); !errors.Is(err, ErrEmailTaken) {
		t.Fatalf("expected ErrEmailTaken, got %v", err)
	}
}
