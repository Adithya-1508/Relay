package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/adithya/relay/pkg/config"
)

// Service-level sentinel errors. Handler maps these to HTTP response codes.
var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrEmailTaken         = errors.New("email already registered")
	ErrTokenInvalid       = errors.New("token invalid")
	ErrTokenExpired       = errors.New("token expired")
	ErrTokenRevoked       = errors.New("token revoked")
)

// Service holds dependencies for the auth domain.
type Service struct {
	repo  Repository
	cfg   config.JWTConfig
	clock func() time.Time
}

// NewService wires the auth Service. clock is the time source; production
// callers pass time.Now, tests pass a fixed source.
func NewService(repo Repository, cfg config.JWTConfig, clock func() time.Time) *Service {
	if clock == nil {
		clock = time.Now
	}
	return &Service{repo: repo, cfg: cfg, clock: clock}
}

// AccessClaims is the JWT payload for an access token.
type AccessClaims struct {
	UserID      uuid.UUID `json:"uid"`
	WorkspaceID uuid.UUID `json:"wsid"`
	Role        Role      `json:"role"`
	jwt.RegisteredClaims
}

// Register creates a workspace and its first user (the owner) and returns
// fresh access+refresh tokens. The whole operation is transactional in the
// repository.
func (s *Service) Register(ctx context.Context, req RegisterRequest, userAgent, ip string) (AuthResponse, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12)
	if err != nil {
		return AuthResponse{}, fmt.Errorf("hash password: %w", err)
	}

	ws := Workspace{Name: req.WorkspaceName, Slug: strings.ToLower(req.WorkspaceSlug)}
	u := User{Email: email, PasswordHash: string(hash)}

	createdWS, createdUser, err := s.repo.CreateWorkspaceAndOwner(ctx, ws, u)
	if err != nil {
		if errors.Is(err, ErrDuplicate) {
			return AuthResponse{}, ErrEmailTaken
		}
		return AuthResponse{}, err
	}

	tokens, err := s.issueTokens(ctx, createdUser, userAgent, ip, uuid.New())
	if err != nil {
		return AuthResponse{}, err
	}

	return AuthResponse{User: createdUser, Workspace: createdWS, Tokens: tokens}, nil
}

// Login authenticates by email+password. To prevent user enumeration the
// caller sees the same ErrInvalidCredentials whether the email is unknown or
// the password is wrong.
func (s *Service) Login(ctx context.Context, req LoginRequest, userAgent, ip string) (AuthResponse, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))

	u, err := s.repo.FindUserByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// Spend bcrypt time anyway to flatten timing differences.
			_ = bcrypt.CompareHashAndPassword([]byte("$2a$12$........................................................"), []byte(req.Password))
			return AuthResponse{}, ErrInvalidCredentials
		}
		return AuthResponse{}, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(req.Password)); err != nil {
		return AuthResponse{}, ErrInvalidCredentials
	}

	ws, err := s.repo.FindWorkspaceByID(ctx, u.WorkspaceID)
	if err != nil {
		return AuthResponse{}, err
	}

	tokens, err := s.issueTokens(ctx, u, userAgent, ip, uuid.New())
	if err != nil {
		return AuthResponse{}, err
	}
	return AuthResponse{User: u, Workspace: ws, Tokens: tokens}, nil
}

// Refresh rotates a refresh token. Returns a fresh access+refresh pair and
// revokes the presented refresh token. If the presented token is already
// revoked, the entire family is killed — that is the "theft detected" path.
func (s *Service) Refresh(ctx context.Context, raw, userAgent, ip string) (AuthResponse, error) {
	hash := hashRefreshToken(raw)

	stored, err := s.repo.FindRefreshTokenByHash(ctx, hash)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return AuthResponse{}, ErrTokenInvalid
		}
		return AuthResponse{}, err
	}

	now := s.clock()
	if stored.Expired(now) {
		return AuthResponse{}, ErrTokenExpired
	}
	if stored.Revoked() {
		// Presenting a revoked token means either the legitimate user replayed
		// an old one (rare) or an attacker stole it. Cheapest correct response:
		// kill the entire family.
		_ = s.repo.RevokeFamily(ctx, stored.FamilyID, now)
		return AuthResponse{}, ErrTokenRevoked
	}

	u, err := s.repo.FindUserByID(ctx, stored.UserID)
	if err != nil {
		return AuthResponse{}, err
	}
	ws, err := s.repo.FindWorkspaceByID(ctx, u.WorkspaceID)
	if err != nil {
		return AuthResponse{}, err
	}

	if err := s.repo.RevokeRefreshToken(ctx, stored.ID, now); err != nil {
		return AuthResponse{}, err
	}

	tokens, err := s.issueTokens(ctx, u, userAgent, ip, stored.FamilyID)
	if err != nil {
		return AuthResponse{}, err
	}
	return AuthResponse{User: u, Workspace: ws, Tokens: tokens}, nil
}

// Me returns the user and their workspace by userID. Used by /v1/me to verify
// the access token round-trips and to give the client a single fetch for
// "who am I right now".
func (s *Service) Me(ctx context.Context, userID uuid.UUID) (User, Workspace, error) {
	u, err := s.repo.FindUserByID(ctx, userID)
	if err != nil {
		return User{}, Workspace{}, err
	}
	ws, err := s.repo.FindWorkspaceByID(ctx, u.WorkspaceID)
	if err != nil {
		return User{}, Workspace{}, err
	}
	return u, ws, nil
}

// Logout revokes a single refresh token. Idempotent — unknown or already
// revoked tokens return nil so the client always sees success.
func (s *Service) Logout(ctx context.Context, raw string) error {
	hash := hashRefreshToken(raw)
	stored, err := s.repo.FindRefreshTokenByHash(ctx, hash)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	if stored.Revoked() {
		return nil
	}
	return s.repo.RevokeRefreshToken(ctx, stored.ID, s.clock())
}

// VerifyAccessToken parses and validates a Bearer access token. Used by the
// auth middleware.
func (s *Service) VerifyAccessToken(raw string) (*AccessClaims, error) {
	parser := jwt.NewParser(jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	claims := &AccessClaims{}
	tok, err := parser.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		return []byte(s.cfg.AccessSecret), nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		return nil, ErrTokenInvalid
	}
	if !tok.Valid {
		return nil, ErrTokenInvalid
	}
	return claims, nil
}

// issueTokens produces an access JWT and a fresh refresh token persisted under
// the given familyID. familyID is the same uuid carried across rotations of a
// single login chain.
func (s *Service) issueTokens(ctx context.Context, u User, userAgent, ip string, familyID uuid.UUID) (TokenPair, error) {
	now := s.clock()

	accessClaims := AccessClaims{
		UserID:      u.ID,
		WorkspaceID: u.WorkspaceID,
		Role:        u.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "relay",
			Subject:   u.ID.String(),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.cfg.AccessExpiry)),
			ID:        uuid.NewString(),
		},
	}
	accessJWT := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)
	access, err := accessJWT.SignedString([]byte(s.cfg.AccessSecret))
	if err != nil {
		return TokenPair{}, fmt.Errorf("sign access token: %w", err)
	}

	rawRefresh, err := randomRefreshToken()
	if err != nil {
		return TokenPair{}, err
	}

	var ua, ipPtr *string
	if userAgent != "" {
		ua = &userAgent
	}
	if ip != "" {
		ipPtr = &ip
	}

	row := RefreshToken{
		ID:        uuid.New(),
		UserID:    u.ID,
		FamilyID:  familyID,
		TokenHash: hashRefreshToken(rawRefresh),
		UserAgent: ua,
		IPAddress: ipPtr,
		ExpiresAt: now.Add(s.cfg.RefreshExpiry),
		CreatedAt: now,
	}
	if err := s.repo.InsertRefreshToken(ctx, row); err != nil {
		return TokenPair{}, err
	}

	return TokenPair{
		AccessToken:  access,
		RefreshToken: rawRefresh,
		TokenType:    "Bearer",
		ExpiresIn:    int(s.cfg.AccessExpiry.Seconds()),
	}, nil
}

// randomRefreshToken returns 32 cryptographically random bytes hex-encoded.
func randomRefreshToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// hashRefreshToken returns the SHA-256 hex digest of a refresh token. We
// never persist raw refresh tokens — only this hash.
func hashRefreshToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
