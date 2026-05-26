package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/adithya/relay/internal/auth"
	"github.com/adithya/relay/pkg/response"
)

// RequireRole gates handlers to one of the supplied roles. Owner implicitly
// satisfies any role below it: owner > admin > member.
func RequireRole(allowed ...auth.Role) gin.HandlerFunc {
	allowSet := make(map[auth.Role]struct{}, len(allowed))
	for _, r := range allowed {
		allowSet[r] = struct{}{}
	}
	// Owner is always allowed wherever admin or member is.
	if _, hasAdmin := allowSet[auth.RoleAdmin]; hasAdmin {
		allowSet[auth.RoleOwner] = struct{}{}
	}
	if _, hasMember := allowSet[auth.RoleMember]; hasMember {
		allowSet[auth.RoleOwner] = struct{}{}
		allowSet[auth.RoleAdmin] = struct{}{}
	}

	return func(c *gin.Context) {
		role, ok := RoleFrom(c)
		if !ok {
			response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "authentication required")
			return
		}
		if _, ok := allowSet[role]; !ok {
			response.Error(c, http.StatusForbidden, response.CodeForbidden, "insufficient permissions")
			return
		}
		c.Next()
	}
}
