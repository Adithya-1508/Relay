package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// requestIDHeader is read from incoming requests if set (so an upstream proxy
// can correlate logs) and is always written back on the response.
const requestIDHeader = "X-Request-ID"

// contextKey is unexported so other packages can't collide on the same key.
type contextKey string

const requestIDKey contextKey = "request_id"

// RequestID assigns a uuid to every request and exposes it via header +
// gin.Context value. RequestIDFromContext retrieves it for logging.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(requestIDHeader)
		if id == "" {
			id = uuid.NewString()
		}
		c.Set(string(requestIDKey), id)
		c.Writer.Header().Set(requestIDHeader, id)
		c.Next()
	}
}

// RequestIDFromContext returns the request id stored on the gin Context, or
// empty string if absent.
func RequestIDFromContext(c *gin.Context) string {
	v, ok := c.Get(string(requestIDKey))
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}
