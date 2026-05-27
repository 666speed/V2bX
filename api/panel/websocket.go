package panel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

type WebSocketHandlers struct {
	OnConnected func(string)
	OnClosed    func(error)
	OnUsers     func([]UserInfo)
	OnConfig    func(json.RawMessage)
}

type wsMessage struct {
	Event     string          `json:"event"`
	Data      json.RawMessage `json:"data"`
	Timestamp int64           `json:"timestamp"`
}

type wsUsersPayload struct {
	Users []UserInfo `json:"users"`
}

// RunWebSocket keeps a best-effort realtime connection to Xboard.
// HTTP polling remains the fallback when the panel does not expose WS.
func (c *Client) RunWebSocket(ctx context.Context, handlers WebSocketHandlers) {
	urls, err := c.webSocketURLs()
	if err != nil {
		log.WithField("err", err).Warn("WebSocket disabled")
		return
	}

	backoff := 5 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var lastErr error
		for _, wsURL := range urls {
			lastErr = c.runWebSocketOnce(ctx, wsURL, handlers)
			if ctx.Err() != nil {
				return
			}
			if lastErr == nil {
				break
			}
		}

		if handlers.OnClosed != nil && lastErr != nil {
			handlers.OnClosed(lastErr)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		if backoff < time.Minute {
			backoff *= 2
			if backoff > time.Minute {
				backoff = time.Minute
			}
		}
	}
}

func (c *Client) runWebSocketOnce(ctx context.Context, wsURL string, handlers WebSocketHandlers) error {
	header := http.Header{}
	header.Set("User-Agent", "V2bX")

	conn, resp, err := c.wsDialer.DialContext(ctx, wsURL, header)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("connect %s: %w", wsURL, err)
	}
	defer conn.Close()

	if handlers.OnConnected != nil {
		handlers.OnConnected(wsURL)
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	for {
		var msg wsMessage
		if err := conn.ReadJSON(&msg); err != nil {
			return err
		}

		switch msg.Event {
		case "auth.success":
			continue
		case "ping":
			if err := conn.WriteJSON(map[string]any{"event": "pong"}); err != nil {
				return err
			}
		case "sync.users":
			users, err := decodeWSUsers(msg.Data)
			if err != nil {
				log.WithField("err", err).Warn("Decode sync.users failed")
				continue
			}
			if handlers.OnUsers != nil {
				handlers.OnUsers(users)
			}
		case "sync.config":
			if handlers.OnConfig != nil {
				handlers.OnConfig(msg.Data)
			}
		case "error":
			return fmt.Errorf("panel websocket error: %s", string(msg.Data))
		}
	}
}

func decodeWSUsers(data json.RawMessage) ([]UserInfo, error) {
	var payload wsUsersPayload
	if err := json.Unmarshal(data, &payload); err == nil && payload.Users != nil {
		return payload.Users, nil
	}

	var users []UserInfo
	if err := json.Unmarshal(data, &users); err != nil {
		return nil, err
	}
	return users, nil
}

func (c *Client) webSocketURLs() ([]string, error) {
	base, err := url.Parse(c.APIHost)
	if err != nil {
		return nil, err
	}
	if base.Host == "" {
		return nil, fmt.Errorf("invalid APIHost: %s", c.APIHost)
	}

	scheme := "ws"
	if base.Scheme == "https" {
		scheme = "wss"
	}

	prefix := strings.TrimRight(base.Path, "/")
	candidates := []string{"/ws", "/ws/", "/api/v2/server/ws"}
	urls := make([]string, 0, len(candidates))
	for _, path := range candidates {
		u := *base
		u.Scheme = scheme
		u.Path = prefix + path
		u.RawQuery = ""
		u.Fragment = ""
		q := u.Query()
		q.Set("node_id", strconv.Itoa(c.NodeId))
		q.Set("node_type", c.NodeType)
		q.Set("token", c.Token)
		u.RawQuery = q.Encode()
		urls = append(urls, u.String())
	}
	return urls, nil
}
