package node

import (
	"context"
	"encoding/json"

	"github.com/InazumaV/V2bX/api/panel"
	vCore "github.com/InazumaV/V2bX/core"
	log "github.com/sirupsen/logrus"
)

func (c *Controller) startWebSocket() {
	if c.wsCancel != nil {
		c.wsCancel()
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.wsCancel = cancel

	go c.apiClient.RunWebSocket(ctx, panel.WebSocketHandlers{
		OnConnected: func(url string) {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"url": url,
			}).Info("WebSocket connected")
		},
		OnClosed: func(err error) {
			if ctx.Err() != nil {
				return
			}
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Debug("WebSocket disconnected, fallback to polling")
		},
		OnUsers: func(users []panel.UserInfo) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.applyUserListLocked(users, "websocket")
		},
		OnConfig: func(_ json.RawMessage) {
			go func() {
				if err := c.nodeInfoMonitor(); err != nil {
					log.WithFields(log.Fields{
						"tag": c.tag,
						"err": err,
					}).Error("WebSocket config refresh failed")
				}
			}()
		},
	})
}

func (c *Controller) applyUserListLocked(newUsers []panel.UserInfo, source string) {
	deleted, added := compareUserList(c.userList, newUsers)
	if len(deleted) > 0 {
		if err := c.server.DelUsers(deleted, c.tag, c.info); err != nil {
			log.WithFields(log.Fields{
				"tag":    c.tag,
				"source": source,
				"err":    err,
			}).Error("Delete users failed")
			return
		}
	}
	if len(added) > 0 {
		_, err := c.server.AddUsers(&vCore.AddUsersParams{
			Tag:      c.tag,
			NodeInfo: c.info,
			Users:    added,
		})
		if err != nil {
			log.WithFields(log.Fields{
				"tag":    c.tag,
				"source": source,
				"err":    err,
			}).Error("Add users failed")
			return
		}
	}
	if len(added) > 0 || len(deleted) > 0 {
		c.limiter.UpdateUser(c.tag, added, deleted)
		if c.LimitConfig.EnableDynamicSpeedLimit {
			for i := range deleted {
				delete(c.traffic, deleted[i].Uuid)
			}
		}
	}
	c.userList = newUsers
	if len(added)+len(deleted) != 0 {
		log.WithFields(log.Fields{
			"tag":     c.tag,
			"source":  source,
			"deleted": len(deleted),
			"added":   len(added),
		}).Info("Users updated")
	}
}

