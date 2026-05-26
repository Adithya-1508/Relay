package job

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/adithya/relay/internal/middleware"
	"github.com/adithya/relay/pkg/response"
)

// Handler binds HTTP routes to the job service.
type Handler struct {
	svc *Service
}

// NewHandler wires the Handler.
func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

// Mount registers authenticated job/pipeline routes.
func (h *Handler) Mount(rg *gin.RouterGroup) {
	rg.POST("/pipelines", h.createPipeline)
	rg.GET("/pipelines", h.listPipelines)
	rg.POST("/pipelines/:slug/jobs", h.enqueueJob)
	rg.GET("/jobs", h.listJobs)
	rg.GET("/jobs/:id", h.getJob)
	rg.GET("/jobs/:id/events", h.listEvents)
}

func (h *Handler) createPipeline(c *gin.Context) {
	workspaceID, ok := middleware.WorkspaceIDFrom(c)
	if !ok {
		response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "auth required")
		return
	}
	var req CreatePipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, http.StatusBadRequest, response.CodeValidation, err.Error())
		return
	}
	p, err := h.svc.CreatePipeline(c.Request.Context(), workspaceID, req)
	switch {
	case errors.Is(err, ErrPipelineExists):
		response.Error(c, http.StatusConflict, response.CodeConflict, "pipeline slug already exists")
		return
	case err != nil:
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "create pipeline failed")
		return
	}
	response.Created(c, p)
}

func (h *Handler) listPipelines(c *gin.Context) {
	workspaceID, ok := middleware.WorkspaceIDFrom(c)
	if !ok {
		response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "auth required")
		return
	}
	pipelines, err := h.svc.ListPipelines(c.Request.Context(), workspaceID)
	if err != nil {
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "list pipelines failed")
		return
	}
	if pipelines == nil {
		pipelines = []Pipeline{}
	}
	response.OK(c, pipelines)
}

func (h *Handler) enqueueJob(c *gin.Context) {
	workspaceID, ok := middleware.WorkspaceIDFrom(c)
	if !ok {
		response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "auth required")
		return
	}
	slug := c.Param("slug")
	var req EnqueueJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, http.StatusBadRequest, response.CodeValidation, err.Error())
		return
	}
	job, err := h.svc.EnqueueJob(c.Request.Context(), workspaceID, slug, req)
	switch {
	case errors.Is(err, ErrPipelineNotFound):
		response.Error(c, http.StatusNotFound, response.CodeNotFound, "pipeline not found")
		return
	case errors.Is(err, ErrUnknownKind):
		response.Error(c, http.StatusBadRequest, response.CodeBadRequest, "unknown job kind")
		return
	case err != nil:
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "enqueue failed")
		return
	}
	response.Created(c, job)
}

func (h *Handler) listJobs(c *gin.Context) {
	workspaceID, ok := middleware.WorkspaceIDFrom(c)
	if !ok {
		response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "auth required")
		return
	}
	req := ListJobsRequest{State: c.Query("state")}
	if l := c.Query("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil {
			req.Limit = n
		}
	}
	if b := c.Query("before"); b != "" {
		if t, err := time.Parse(time.RFC3339Nano, b); err == nil {
			req.BeforeAt = &t
		}
	}
	if bid := c.Query("before_id"); bid != "" {
		if id, err := uuid.Parse(bid); err == nil {
			req.BeforeID = &id
		}
	}
	page, err := h.svc.ListJobs(c.Request.Context(), workspaceID, req)
	if err != nil {
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "list jobs failed")
		return
	}
	if page.Jobs == nil {
		page.Jobs = []Job{}
	}
	response.OK(c, page)
}

func (h *Handler) getJob(c *gin.Context) {
	workspaceID, ok := middleware.WorkspaceIDFrom(c)
	if !ok {
		response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "auth required")
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		response.Error(c, http.StatusBadRequest, response.CodeBadRequest, "invalid job id")
		return
	}
	job, err := h.svc.GetJob(c.Request.Context(), workspaceID, id)
	switch {
	case errors.Is(err, ErrJobNotFound):
		response.Error(c, http.StatusNotFound, response.CodeNotFound, "job not found")
		return
	case err != nil:
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "get job failed")
		return
	}
	response.OK(c, job)
}

func (h *Handler) listEvents(c *gin.Context) {
	workspaceID, ok := middleware.WorkspaceIDFrom(c)
	if !ok {
		response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "auth required")
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		response.Error(c, http.StatusBadRequest, response.CodeBadRequest, "invalid job id")
		return
	}
	events, err := h.svc.ListEvents(c.Request.Context(), workspaceID, id)
	switch {
	case errors.Is(err, ErrJobNotFound):
		response.Error(c, http.StatusNotFound, response.CodeNotFound, "job not found")
		return
	case err != nil:
		response.Error(c, http.StatusInternalServerError, response.CodeInternal, "list events failed")
		return
	}
	if events == nil {
		events = []Event{}
	}
	response.OK(c, events)
}
