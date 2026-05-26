package middleware

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/adithya/relay/internal/auth"
	"github.com/adithya/relay/pkg/response"
)

// Context keys for values set by RequireAuth. Other handlers read via the
// exported helpers below rather than reaching into the gin context directly.
const (
	ctxUserID      = "auth_user_id"
	ctxWorkspaceID = "auth_workspace_id"
	ctxRole        = "auth_role"
)

// RequireAuth parses the Authorization: Bearer <jwt> header, validates the
// token against the auth service, and stuffs user/workspace/role onto the
// context for downstream handlers. Aborts with 401 on any failure.
func RequireAuth(svc *auth.Service) gin.HandlerFunc {
	return requireAuthCommon(svc, false)
}

// RequireAuthWithQueryToken is RequireAuth but also accepts the token as
// ?token=<jwt> query param. Use ONLY for WebSocket upgrade endpoints —
// browsers can't set custom Authorization headers on WS open. Query tokens
// are otherwise discouraged because they leak into access logs.
func RequireAuthWithQueryToken(svc *auth.Service) gin.HandlerFunc {
	return requireAuthCommon(svc, true)
}

func requireAuthCommon(svc *auth.Service, allowQueryFallback bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		raw := bearerToken(c.GetHeader("Authorization"))
		if raw == "" && allowQueryFallback {
			raw = c.Query("token")
		}
		if raw == "" {
			response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "missing bearer token")
			return
		}

		claims, err := svc.VerifyAccessToken(raw)
		if err != nil {
			switch {
			case errors.Is(err, auth.ErrTokenExpired):
				response.Error(c, http.StatusUnauthorized, response.CodeTokenExpired, "access token expired")
			default:
				response.Error(c, http.StatusUnauthorized, response.CodeTokenInvalid, "access token invalid")
			}
			return
		}

		c.Set(ctxUserID, claims.UserID)
		c.Set(ctxWorkspaceID, claims.WorkspaceID)
		c.Set(ctxRole, claims.Role)
		c.Next()
	}
}

// UserIDFrom returns the authenticated user id from the gin context. The
// boolean is false if RequireAuth did not run.
func UserIDFrom(c *gin.Context) (uuid.UUID, bool) {
	v, ok := c.Get(ctxUserID)
	if !ok {
		return uuid.Nil, false
	}
	id, ok := v.(uuid.UUID)
	return id, ok
}

// WorkspaceIDFrom returns the authenticated workspace id from the gin context.
func WorkspaceIDFrom(c *gin.Context) (uuid.UUID, bool) {
	v, ok := c.Get(ctxWorkspaceID)
	if !ok {
		return uuid.Nil, false
	}
	id, ok := v.(uuid.UUID)
	return id, ok
}

// RoleFrom returns the authenticated user's role.
func RoleFrom(c *gin.Context) (auth.Role, bool) {
	v, ok := c.Get(ctxRole)
	if !ok {
		return "", false
	}
	r, ok := v.(auth.Role)
	return r, ok
}

// bearerToken extracts the token out of an "Authorization: Bearer xxx" header.
func bearerToken(h string) string {
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}
