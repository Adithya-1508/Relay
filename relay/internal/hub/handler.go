package hub

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/adithya/relay/internal/middleware"
	"github.com/adithya/relay/pkg/response"
)

// upgrader is shared by all WS connections. CheckOrigin returns true because
// the API is meant to be hit by SPA frontends; tighten this in production by
// validating the Origin header against an allowed list.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(*http.Request) bool { return true },
}

// Handler binds a Gin route that upgrades to WebSocket and subscribes the
// connection to its workspace topic.
type Handler struct {
	hub *Hub
}

// NewHandler builds a hub Handler.
func NewHandler(h *Hub) *Handler { return &Handler{hub: h} }

// Mount wires GET /ws/jobs on the supplied (already-authenticated) router
// group.
func (h *Handler) Mount(rg *gin.RouterGroup) {
	rg.GET("/ws/jobs", h.subscribe)
}

func (h *Handler) subscribe(c *gin.Context) {
	workspaceID, ok := middleware.WorkspaceIDFrom(c)
	if !ok {
		response.Error(c, http.StatusUnauthorized, response.CodeUnauthorized, "auth required")
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		// upgrader.Upgrade has already written a 4xx; no further response.
		return
	}

	logger := h.hub.log.With("workspace", workspaceID)
	cl := newClient(conn, logger)
	topic := WorkspaceTopic(workspaceID.String())

	ctx, cancel := context.WithCancel(c.Request.Context())
	h.hub.Subscribe(ctx, topic, cl)

	// Both pumps share the cancel; whichever exits first kills the other.
	go func() {
		defer h.hub.Unsubscribe(topic, cl.id())
		cl.writePump(ctx)
	}()
	go cl.readPump(cancel)
}
