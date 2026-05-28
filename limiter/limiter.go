package limiter

import (
	"errors"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/format"
	"github.com/InazumaV/V2bX/conf"
	"github.com/juju/ratelimit"
)

var limitLock sync.RWMutex
var limiter map[string]*Limiter

func Init() {
	limiter = map[string]*Limiter{}
}

type Limiter struct {
	DomainRules   []*regexp.Regexp
	ProtocolRules []string
	SpeedLimit    int
	UserOnlineIP  *sync.Map      // Key: TagUUID, value: {Key: Ip, value: Uid}
	OldUserOnline *sync.Map      // Key: Ip, value: Uid
	UUIDtoUID     map[string]int // Key: UUID, value: Uid
	UserLimitInfo *sync.Map      // Key: TagUUID value: UserLimitInfo
	SpeedLimiter  *sync.Map      // key: TagUUID, value: *ratelimit.Bucket
	AliveList     map[int]int    // Key: Uid, value: alive_ip
}

type UserLimitInfo struct {
	UID               int
	SpeedLimit        int
	DeviceLimit       int
	DynamicSpeedLimit int
	ExpireTime        int64
	OverLimit         bool
	WhitelistEnabled  bool
	WhitelistCIDRs    []*net.IPNet
}

func AddLimiter(tag string, l *conf.LimitConfig, users []panel.UserInfo, aliveList map[int]int) *Limiter {
	info := &Limiter{
		SpeedLimit:    l.SpeedLimit,
		UserOnlineIP:  new(sync.Map),
		UserLimitInfo: new(sync.Map),
		SpeedLimiter:  new(sync.Map),
		AliveList:     aliveList,
		OldUserOnline: new(sync.Map),
	}
	uuidmap := make(map[string]int)
	for i := range users {
		uuidmap[users[i].Uuid] = users[i].Id
		userLimit := buildUserLimitInfo(users[i])
		info.UserLimitInfo.Store(format.UserTag(tag, users[i].Uuid), userLimit)
	}
	info.UUIDtoUID = uuidmap
	limitLock.Lock()
	limiter[tag] = info
	limitLock.Unlock()
	return info
}

func GetLimiter(tag string) (info *Limiter, err error) {
	limitLock.RLock()
	info, ok := limiter[tag]
	limitLock.RUnlock()
	if !ok {
		return nil, errors.New("not found")
	}
	return info, nil
}

func DeleteLimiter(tag string) {
	limitLock.Lock()
	delete(limiter, tag)
	limitLock.Unlock()
}

func (l *Limiter) UserID(taguuid string) (int, bool) {
	if v, ok := l.UserLimitInfo.Load(taguuid); ok {
		return v.(*UserLimitInfo).UID, true
	}
	return 0, false
}

func (l *Limiter) UpdateUser(tag string, added []panel.UserInfo, deleted []panel.UserInfo) {
	for i := range deleted {
		l.UserLimitInfo.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.UserOnlineIP.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.SpeedLimiter.Delete(format.UserTag(tag, deleted[i].Uuid))
		delete(l.UUIDtoUID, deleted[i].Uuid)
		delete(l.AliveList, deleted[i].Id)
	}
	for i := range added {
		userLimit := buildUserLimitInfo(added[i])
		l.UserLimitInfo.Store(format.UserTag(tag, added[i].Uuid), userLimit)
		l.UUIDtoUID[added[i].Uuid] = added[i].Id
	}
}

func (l *Limiter) UpdateDynamicSpeedLimit(tag, uuid string, limit int, expire time.Time) error {
	if v, ok := l.UserLimitInfo.Load(format.UserTag(tag, uuid)); ok {
		info := v.(*UserLimitInfo)
		info.DynamicSpeedLimit = limit
		info.ExpireTime = expire.Unix()
	} else {
		return errors.New("not found")
	}
	return nil
}

func (l *Limiter) CheckLimit(taguuid string, ip string, isTcp bool, noSSUDP bool) (Bucket *ratelimit.Bucket, Reject bool) {
	// check if ipv4 mapped ipv6
	ip = normalizeSourceIP(ip)

	// check and gen speed limit Bucket
	nodeLimit := l.SpeedLimit
	userLimit := 0
	deviceLimit := 0
	var uid int
	if v, ok := l.UserLimitInfo.Load(taguuid); ok {
		u := v.(*UserLimitInfo)
		deviceLimit = u.DeviceLimit
		uid = u.UID
		if !u.WhitelistEnabled || !checkWhitelist(ip, u.WhitelistCIDRs) {
			return nil, true
		}
		if u.ExpireTime < time.Now().Unix() && u.ExpireTime != 0 {
			if u.SpeedLimit != 0 {
				userLimit = u.SpeedLimit
				u.DynamicSpeedLimit = 0
				u.ExpireTime = 0
			} else {
				l.UserLimitInfo.Delete(taguuid)
			}
		} else {
			userLimit = determineSpeedLimit(u.SpeedLimit, u.DynamicSpeedLimit)
		}
	} else {
		return nil, true
	}
	if noSSUDP {
		// Store online user for device limit
		newipMap := new(sync.Map)
		newipMap.Store(ip, uid)
		aliveIp := l.AliveList[uid]
		// If any device is online
		if v, loaded := l.UserOnlineIP.LoadOrStore(taguuid, newipMap); loaded {
			oldipMap := v.(*sync.Map)
			// If this is a new ip
			if _, loaded := oldipMap.LoadOrStore(ip, uid); !loaded {
				if v, loaded := l.OldUserOnline.Load(ip); loaded {
					if v.(int) == uid {
						l.OldUserOnline.Delete(ip)
					}
				} else if deviceLimit > 0 {
					if deviceLimit <= aliveIp {
						oldipMap.Delete(ip)
						return nil, true
					}
				}
			}
		} else if v, ok := l.OldUserOnline.Load(ip); ok {
			if v.(int) == uid {
				l.OldUserOnline.Delete(ip)
			}
		} else {
			if deviceLimit > 0 {
				if deviceLimit <= aliveIp {
					l.UserOnlineIP.Delete(taguuid)
					return nil, true
				}
			}
		}
	}

	limit := int64(determineSpeedLimit(nodeLimit, userLimit)) * 1000000 / 8 // If you need the Speed limit
	if limit > 0 {
		Bucket = ratelimit.NewBucketWithQuantum(time.Second, limit, limit) // Byte/s
		if v, ok := l.SpeedLimiter.LoadOrStore(taguuid, Bucket); ok {
			return v.(*ratelimit.Bucket), false
		} else {
			l.SpeedLimiter.Store(taguuid, Bucket)
			return Bucket, false
		}
	} else {
		return nil, false
	}
}

func (l *Limiter) GetOnlineDevice() (*[]panel.OnlineUser, error) {
	var onlineUser []panel.OnlineUser
	l.OldUserOnline = new(sync.Map)
	l.UserOnlineIP.Range(func(key, value interface{}) bool {
		taguuid := key.(string)
		ipMap := value.(*sync.Map)
		ipMap.Range(func(key, value interface{}) bool {
			uid := value.(int)
			ip := key.(string)
			l.OldUserOnline.Store(ip, uid)
			onlineUser = append(onlineUser, panel.OnlineUser{UID: uid, IP: ip})
			return true
		})
		l.UserOnlineIP.Delete(taguuid) // Reset online device
		return true
	})

	return &onlineUser, nil
}

type UserIpList struct {
	Uid    int      `json:"Uid"`
	IpList []string `json:"Ips"`
}

func buildUserLimitInfo(user panel.UserInfo) *UserLimitInfo {
	userLimit := &UserLimitInfo{
		UID:              user.Id,
		WhitelistEnabled: user.IPWhitelistEnabled,
		WhitelistCIDRs:   parseWhitelist(user.IPWhitelist),
		OverLimit:        false,
	}
	if user.SpeedLimit != 0 {
		userLimit.SpeedLimit = user.SpeedLimit
		userLimit.ExpireTime = 0
	}
	if user.DeviceLimit != 0 {
		userLimit.DeviceLimit = user.DeviceLimit
	}
	return userLimit
}

func CheckIPWhitelist(ip string, whitelist []string) bool {
	return checkWhitelist(normalizeSourceIP(ip), parseWhitelist(whitelist))
}

func checkWhitelist(ip string, whitelist []*net.IPNet) bool {
	parsed := net.ParseIP(normalizeSourceIP(ip))
	if parsed == nil || len(whitelist) == 0 {
		return false
	}
	for _, network := range whitelist {
		if network.Contains(parsed) {
			return true
		}
	}
	return false
}

func parseWhitelist(entries []string) []*net.IPNet {
	networks := make([]*net.IPNet, 0, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if strings.Contains(entry, "/") {
			_, network, err := net.ParseCIDR(entry)
			if err == nil && network != nil {
				networks = append(networks, network)
			}
			continue
		}
		ip := net.ParseIP(normalizeSourceIP(entry))
		if ip == nil {
			continue
		}
		prefix := 128
		if ip.To4() != nil {
			prefix = 32
		}
		networks = append(networks, &net.IPNet{
			IP:   ip,
			Mask: net.CIDRMask(prefix, prefix),
		})
	}
	return networks
}

func normalizeSourceIP(ip string) string {
	ip = strings.TrimSpace(strings.TrimPrefix(ip, "::ffff:"))
	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}
	if strings.HasPrefix(ip, "[") && strings.Contains(ip, "]") {
		ip = strings.Trim(ip, "[]")
	}
	if parsed := net.ParseIP(ip); parsed != nil {
		return parsed.String()
	}
	if idx := strings.LastIndex(ip, ":"); idx > 0 && strings.Contains(ip[:idx], ".") {
		if parsed := net.ParseIP(ip[:idx]); parsed != nil {
			return parsed.String()
		}
	}
	return ip
}
