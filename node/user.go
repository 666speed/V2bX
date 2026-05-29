package node

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/usage"
	log "github.com/sirupsen/logrus"
)

const maxIPUsageReportItems = 1000
const maxIPUsagePendingBatches = 60
const maxIPUsagePendingAge = 10 * time.Minute
const reportFailureLogInterval = 10 * time.Minute

type ipUsageReportBatch struct {
	ID         string
	ReportedAt int64
	CreatedAt  int64
	Items      []panel.IPUsageReport
}

func (c *Controller) reportUserTrafficTask() (err error) {
	userTraffic, _ := c.server.GetUserTrafficSlice(c.tag, true)
	if len(userTraffic) > 0 {
		err = c.apiClient.ReportUserTraffic(userTraffic)
		if err != nil {
			c.logReportFailure(&c.lastTrafficFailureLog, "Report user traffic failed", log.Fields{
				"tag": c.tag,
				"err": err,
			})
		} else {
			log.WithField("tag", c.tag).Debugf("Report %d users traffic", len(userTraffic))
			log.WithField("tag", c.tag).Debugf("User traffic: %+v", userTraffic)
		}
	}

	if onlineDevice, err := c.limiter.GetOnlineDevice(); err != nil {
		c.logReportFailure(&c.lastOnlineFailureLog, "Get online devices failed", log.Fields{
			"tag": c.tag,
			"err": err,
		})
	} else if len(*onlineDevice) > 0 {
		// Only report user has traffic > 100kb to allow ping test
		var result []panel.OnlineUser
		var nocountUID = make(map[int]struct{})
		for _, traffic := range userTraffic {
			total := traffic.Upload + traffic.Download
			if total < int64(c.Options.DeviceOnlineMinTraffic*1000) {
				nocountUID[traffic.UID] = struct{}{}
			}
		}
		for _, online := range *onlineDevice {
			if _, ok := nocountUID[online.UID]; !ok {
				result = append(result, online)
			}
		}
		data := make(map[int][]string)
		for _, onlineuser := range result {
			// json structure: { UID1:["ip1","ip2"],UID2:["ip3","ip4"] }
			data[onlineuser.UID] = append(data[onlineuser.UID], onlineuser.IP)
		}
		if err = c.apiClient.ReportNodeOnlineUsers(&data); err != nil {
			c.logReportFailure(&c.lastOnlineFailureLog, "Report online users failed", log.Fields{
				"tag": c.tag,
				"err": err,
			})
		} else {
			log.WithField("tag", c.tag).Debugf("Total %d online users, %d Reported", len(*onlineDevice), len(result))
			log.WithField("tag", c.tag).Debugf("Online users: %+v", data)
		}
	}

	pendingClear := c.flushIPUsagePending()

	ipUsage := usage.Snapshot(c.tag, true)
	if len(ipUsage) > 0 {
		reported := 0
		for start := 0; start < len(ipUsage); start += maxIPUsageReportItems {
			end := start + maxIPUsageReportItems
			if end > len(ipUsage) {
				end = len(ipUsage)
			}
			chunk := ipUsage[start:end]
			batch := ipUsageReportBatch{
				ID:         c.newIPUsageReportID(start),
				ReportedAt: time.Now().Unix(),
				CreatedAt:  time.Now().Unix(),
				Items:      make([]panel.IPUsageReport, 0, len(chunk)),
			}
			for _, item := range chunk {
				batch.Items = append(batch.Items, panel.IPUsageReport{
					UID:         item.UID,
					IP:          item.IP,
					Connections: item.Connections,
					Upload:      item.Upload,
					Download:    item.Download,
				})
			}
			if !pendingClear {
				c.queueIPUsagePending(batch)
				continue
			}
			if err = c.apiClient.ReportIPUsage(batch.ID, batch.ReportedAt, batch.Items); err != nil {
				c.queueIPUsagePending(batch)
				c.logReportFailure(&c.lastIPUsageFailureLog, "Report IP usage chunk failed", log.Fields{
					"tag":   c.tag,
					"err":   err,
					"start": start,
					"count": len(chunk),
				})
				continue
			}
			reported += len(batch.Items)
		}
		if reported > 0 {
			log.WithField("tag", c.tag).Debugf("Report %d IP usage rows", reported)
		}
	}

	userTraffic = nil
	return nil
}

func (c *Controller) flushIPUsagePending() bool {
	dropped := c.pruneIPUsagePending()
	if dropped > 0 {
		c.logReportFailure(&c.lastIPUsageFailureLog, "Drop stale IP usage report batches", log.Fields{
			"tag":     c.tag,
			"dropped": dropped,
		})
	}
	if len(c.ipUsagePending) == 0 {
		return true
	}

	pending := c.ipUsagePending
	c.ipUsagePending = nil
	reported := 0
	for index, batch := range pending {
		if err := c.apiClient.ReportIPUsage(batch.ID, batch.ReportedAt, batch.Items); err != nil {
			c.ipUsagePending = append(c.ipUsagePending, pending[index:]...)
			c.pruneIPUsagePending()
			c.logReportFailure(&c.lastIPUsageFailureLog, "Retry IP usage report failed", log.Fields{
				"tag":       c.tag,
				"err":       err,
				"report_id": batch.ID,
				"count":     len(batch.Items),
			})
			return false
		}
		reported += len(batch.Items)
	}
	if reported > 0 {
		log.WithField("tag", c.tag).Debugf("Report %d pending IP usage rows", reported)
	}
	return len(c.ipUsagePending) == 0
}

func (c *Controller) queueIPUsagePending(batch ipUsageReportBatch) {
	if len(batch.Items) == 0 {
		return
	}
	if batch.CreatedAt <= 0 {
		batch.CreatedAt = time.Now().Unix()
	}
	c.ipUsagePending = append(c.ipUsagePending, batch)
	if dropped := c.pruneIPUsagePending(); dropped > 0 {
		c.logReportFailure(&c.lastIPUsageFailureLog, "Drop overflow IP usage report batches", log.Fields{
			"tag":     c.tag,
			"dropped": dropped,
			"kept":    len(c.ipUsagePending),
		})
	}
}

func (c *Controller) pruneIPUsagePending() int {
	if len(c.ipUsagePending) == 0 {
		return 0
	}

	now := time.Now().Unix()
	maxAgeSeconds := int64(maxIPUsagePendingAge / time.Second)
	kept := c.ipUsagePending[:0]
	dropped := 0
	for _, batch := range c.ipUsagePending {
		if batch.CreatedAt > 0 && now-batch.CreatedAt > maxAgeSeconds {
			dropped++
			continue
		}
		kept = append(kept, batch)
	}

	if len(kept) > maxIPUsagePendingBatches {
		overflow := len(kept) - maxIPUsagePendingBatches
		dropped += overflow
		kept = kept[overflow:]
	}

	c.ipUsagePending = kept
	return dropped
}

func (c *Controller) logReportFailure(last *time.Time, message string, fields log.Fields) {
	now := time.Now()
	if !last.IsZero() && now.Sub(*last) < reportFailureLogInterval {
		return
	}
	*last = now
	log.WithFields(fields).Warn(message)
}

func (c *Controller) newIPUsageReportID(offset int) string {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("%d-%d-%d", c.apiClient.NodeId, time.Now().UnixNano(), offset)
	}
	return fmt.Sprintf("%d-%d-%d-%s", c.apiClient.NodeId, time.Now().UnixNano(), offset, hex.EncodeToString(random))
}

func compareUserList(old, new []panel.UserInfo) (deleted, added []panel.UserInfo) {
	oldMap := make(map[string]int)
	for i, user := range old {
		key := userCompareKey(user)
		oldMap[key] = i
	}

	for _, user := range new {
		key := userCompareKey(user)
		if _, exists := oldMap[key]; !exists {
			added = append(added, user)
		} else {
			delete(oldMap, key)
		}
	}

	for _, index := range oldMap {
		deleted = append(deleted, old[index])
	}

	return deleted, added
}

func userCompareKey(user panel.UserInfo) string {
	whitelist := append([]string(nil), user.IPWhitelist...)
	sort.Strings(whitelist)
	return strings.Join([]string{
		user.Uuid,
		strconv.Itoa(user.SpeedLimit),
		strconv.Itoa(user.DeviceLimit),
		strconv.FormatBool(user.IPWhitelistEnabled),
		strconv.FormatBool(user.IPWhitelistBypass),
		strings.Join(whitelist, ","),
	}, "|")
}
