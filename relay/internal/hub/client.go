package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Per-connection settings. WebSocket needs a write deadline + periodic pings
// so dead TCP connections get reaped quickly.
const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second // must be < pongWait
	clientSendBuf  = 256              // bounded per-client outbound queue
	maxMessageSize = 4 * 1024
)

// client wraps a single WebSocket connection. send() pushes onto a bounded
// channel; if the channel is full the client is considered slow and dropped.
// Each client owns one read pump (drops anything from the user) and one write
// pump (drains the outbound channel + heartbeats).
type client struct {
	connID   string
	conn     *websocket.Conn
	outbound chan Message
	log      *slog.Logger
}

func newClient(conn *websocket.Conn, log *slog.Logger) *client {
	return &client{
		connID:   uuid.NewString(),
		conn:     conn,
		outbound: make(chan Message, clientSendBuf),
		log:      log,
	}
}

func (c *client) id() string { return c.connID }

// send implements subscriber.send. Non-blocking: returns false (drop me) if
// the outbound buffer is full.
func (c *client) send(m Message) bool {
	select {
	case c.outbound <- m:
		return true
	default:
		return false
	}
}

// readPump consumes (and discards) frames from the client. We have no inbound
// API today, but we still need to read so ping/pong work and SetReadDeadline
// triggers when the client goes silent.
func (c *client) readPump(cancel context.CancelFunc) {
	defer func() {
		cancel()
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.log.Warn("ws read error", "conn", c.connID, "error", err)
			}
			return
		}
	}
}

// writePump drains the outbound channel onto the socket and emits periodic
// pings. Returns when ctx is cancelled (e.g. read pump errored) or the
// channel closes.
func (c *client) writePump(ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case <-ctx.Done():
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "shutdown"))
			return
		case msg, ok := <-c.outbound:
			if !ok {
				return
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			payload, err := json.Marshal(msg)
			if err != nil {
				c.log.Warn("ws marshal", "error", err)
				continue
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				c.log.Warn("ws write", "conn", c.connID, "error", err)
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
