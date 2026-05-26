// Package response provides a single JSON envelope used by every HTTP handler.
//
// Wire shape:
//
//	{ "status": "ok",    "data": {...} }
//	{ "status": "error", "error": { "code": "SLUG", "message": "human readable" } }
//
// Centralising this means handlers never hand-roll JSON and clients can rely
// on a stable error contract.
package response

import (
	"github.com/gin-gonic/gin"
)

// ErrorBody is the structured error payload sent under the "error" key.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Envelope is the success/error wire envelope. Exactly one of Data or Error is
// populated.
type Envelope struct {
	Status string     `json:"status"`
	Data   any        `json:"data,omitempty"`
	Error  *ErrorBody `json:"error,omitempty"`
}

// OK writes a 200 success envelope with the given payload.
func OK(c *gin.Context, data any) {
	c.JSON(200, Envelope{Status: "ok", Data: data})
}

// Created writes a 201 success envelope.
func Created(c *gin.Context, data any) {
	c.JSON(201, Envelope{Status: "ok", Data: data})
}

// NoContent writes a 204 with no body. Used for logout-style endpoints.
func NoContent(c *gin.Context) {
	c.Status(204)
}

// Error writes an error envelope at the given HTTP status with a stable
// machine-readable code and a human message. AbortWithStatusJSON stops the
// middleware chain so later handlers can't accidentally write another body.
func Error(c *gin.Context, status int, code, message string) {
	c.AbortWithStatusJSON(status, Envelope{
		Status: "error",
		Error:  &ErrorBody{Code: code, Message: message},
	})
}

// Common error codes used across handlers. Defined here so callers refer to a
// constant rather than re-typing the slug.
const (
	CodeBadRequest      = "bad_request"
	CodeValidation      = "validation_failed"
	CodeUnauthorized    = "unauthorized"
	CodeForbidden       = "forbidden"
	CodeNotFound        = "not_found"
	CodeConflict        = "conflict"
	CodeRateLimited     = "rate_limited"
	CodeInternal        = "internal_error"
	CodeInvalidCreds    = "invalid_credentials"
	CodeTokenInvalid    = "token_invalid"
	CodeTokenExpired    = "token_expired"
)
