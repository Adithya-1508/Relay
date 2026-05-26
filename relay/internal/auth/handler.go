package auth

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/adithya/relay/pkg/response"
)

// Handler binds HTTP routes to the auth service.
type Handler struct {
	svc *Service
}

// NewHandler wires the Handler.
func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

// Mount registers the unauthenticated auth routes on the given router group.
func (h *Handler) Mount(rg *gin.RouterGroup) {
	rg.POST("/register", h.register)
	rg.POST("/login", h.login)
	rg.POST("/refresh", h.refresh)
	rg.POST("/logout", h.logout)
}

// Me handles the authenticated /v1/me endpoint. Caller must already have
// passed the RequireAuth middleware so the user id is on the context.
func (h *Handler) Me(c *gin.Context) {
	v, ok := c.Get("auth_user_id")
	if !ok {
		response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "auth required")
		return
	}
	id, ok := v.(uuid.UUID)
	if !ok {
		response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "auth required")
		return
	}
	user, ws, err := h.svc.Me(c.Request.Context(), id)
	if err != nil {
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "lookup failed")
		return
	}
	response.OK(c, gin.H{"user": user, "workspace": ws})
}

func (h *Handler) register(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, http.StatusBadRequest, response.CodeValidation, err.Error())
		return
	}

	res, err := h.svc.Register(c.Request.Context(), req, c.Request.UserAgent(), c.ClientIP())
	switch {
	case errors.Is(err, ErrEmailTaken):
		response.Error(c, http.StatusConflict, response.CodeConflict, "email or workspace slug already in use")
		return
	case err != nil:
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "registration failed")
		return
	}
	response.Created(c, res)
}

func (h *Handler) login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, http.StatusBadRequest, response.CodeValidation, err.Error())
		return
	}

	res, err := h.svc.Login(c.Request.Context(), req, c.Request.UserAgent(), c.ClientIP())
	switch {
	case errors.Is(err, ErrInvalidCredentials):
		response.Error(c, http.StatusUnauthorized, response.CodeInvalidCreds, "invalid email or password")
		return
	case err != nil:
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "login failed")
		return
	}
	response.OK(c, res)
}

func (h *Handler) refresh(c *gin.Context) {
	var req RefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, http.StatusBadRequest, response.CodeValidation, err.Error())
		return
	}

	res, err := h.svc.Refresh(c.Request.Context(), req.RefreshToken, c.Request.UserAgent(), c.ClientIP())
	switch {
	case errors.Is(err, ErrTokenInvalid):
		response.Error(c, http.StatusUnauthorized, response.CodeTokenInvalid, "refresh token invalid")
		return
	case errors.Is(err, ErrTokenExpired):
		response.Error(c, http.StatusUnauthorized, response.CodeTokenExpired, "refresh token expired")
		return
	case errors.Is(err, ErrTokenRevoked):
		response.Error(c, http.StatusUnauthorized, response.CodeTokenInvalid, "refresh token revoked")
		return
	case err != nil:
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "refresh failed")
		return
	}
	response.OK(c, res)
}

func (h *Handler) logout(c *gin.Context) {
	var req LogoutRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, http.StatusBadRequest, response.CodeValidation, err.Error())
		return
	}

	if err := h.svc.Logout(c.Request.Context(), req.RefreshToken); err != nil {
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "logout failed")
		return
	}
	response.NoContent(c)
}
