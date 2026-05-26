package auth

// RegisterRequest is the body for POST /v1/auth/register.
type RegisterRequest struct {
	Email         string `json:"email"          binding:"required,email"`
	Password      string `json:"password"       binding:"required,min=8,max=128"`
	WorkspaceName string `json:"workspace_name" binding:"required,min=2,max=64"`
	WorkspaceSlug string `json:"workspace_slug" binding:"required,min=2,max=64,alphanum|contains=-"`
}

// LoginRequest is the body for POST /v1/auth/login.
type LoginRequest struct {
	Email    string `json:"email"    binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

// RefreshRequest is the body for POST /v1/auth/refresh.
type RefreshRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

// LogoutRequest is the body for POST /v1/auth/logout.
type LogoutRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

// TokenPair is the access+refresh pair returned by register/login/refresh.
type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
}

// AuthResponse wraps a user/workspace summary plus the token pair.
type AuthResponse struct {
	User      User      `json:"user"`
	Workspace Workspace `json:"workspace"`
	Tokens    TokenPair `json:"tokens"`
}
