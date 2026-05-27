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

type ipUsageReportBatch struct {
	ID         string
	ReportedAt int64
	Items      []panel.IPUsageReport
}

func (c *Controller) reportUserTrafficTask() (err error) {
	userTraffic, _ := c.server.GetUserTrafficSlice(c.tag, true)
	if len(userTraffic) > 0 {
		err = c.apiClient.ReportUserTraffic(userTraffic)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Info("Report user traffic failed")
		} else {
			log.WithField("tag", c.tag).Infof("Report %d users traffic", len(userTraffic))
			log.WithField("tag", c.tag).Debugf("User traffic: %+v", userTraffic)
		}
	}

	if onlineDevice, err := c.limiter.GetOnlineDevice(); err != nil {
		log.Print(err)
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
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Info("Report online users failed")
		} else {
			log.WithField("tag", c.tag).Infof("Total %d online users, %d Reported", len(*onlineDevice), len(result))
			log.WithField("tag", c.tag).Debugf("Online users: %+v", data)
		}
	}

	if !c.flushIPUsagePending() {
		userTraffic = nil
		return nil
	}

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
			if err = c.apiClient.ReportIPUsage(batch.ID, batch.ReportedAt, batch.Items); err != nil {
				c.ipUsagePending = append(c.ipUsagePending, batch)
				log.WithFields(log.Fields{
					"tag":   c.tag,
					"err":   err,
					"start": start,
					"count": len(chunk),
				}).Info("Report IP usage chunk failed")
				continue
			}
			reported += len(batch.Items)
		}
		if reported > 0 {
			log.WithField("tag", c.tag).Infof("Report %d IP usage rows", reported)
		}
	}

	userTraffic = nil
	return nil
}

func (c *Controller) flushIPUsagePending() bool {
	if len(c.ipUsagePending) == 0 {
		return true
	}

	pending := c.ipUsagePending
	c.ipUsagePending = nil
	reported := 0
	for _, batch := range pending {
		if err := c.apiClient.ReportIPUsage(batch.ID, batch.ReportedAt, batch.Items); err != nil {
			c.ipUsagePending = append(c.ipUsagePending, batch)
			log.WithFields(log.Fields{
				"tag":       c.tag,
				"err":       err,
				"report_id": batch.ID,
				"count":     len(batch.Items),
			}).Info("Retry IP usage report failed")
			continue
		}
		reported += len(batch.Items)
	}
	if reported > 0 {
		log.WithField("tag", c.tag).Infof("Report %d pending IP usage rows", reported)
	}
	return len(c.ipUsagePending) == 0
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
		strings.Join(whitelist, ","),
	}, "|")
}
