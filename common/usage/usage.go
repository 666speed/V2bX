package usage

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/InazumaV/V2bX/common/counter"
)

type Stat struct {
	UID         int
	IP          string
	Connections int64
	Upload      int64
	Download    int64
}

type usageKey struct {
	Tag string
	UID int
	IP  string
}

type usageStat struct {
	tag         string
	uid         int
	ip          string
	connections atomic.Int64
	traffic     *counter.TrafficStorage
}

var (
	stats      sync.Map
	lastUserIP sync.Map
)

func RecordConnection(tag string, uid int, ip string) {
	stat := getStat(tag, uid, ip)
	if stat == nil {
		return
	}
	stat.connections.Add(1)
	RememberUserIP(tag, uid, ip)
}

func Traffic(tag string, uid int, ip string) *counter.TrafficStorage {
	stat := getStat(tag, uid, ip)
	if stat == nil {
		return nil
	}
	return stat.traffic
}

func AddTraffic(tag string, uid int, ip string, upload int64, download int64) {
	stat := getStat(tag, uid, ip)
	if stat == nil {
		return
	}
	if upload > 0 {
		stat.traffic.UpCounter.Add(upload)
	}
	if download > 0 {
		stat.traffic.DownCounter.Add(download)
	}
}

func RememberUserIP(tag string, uid int, ip string) {
	ip = normalizeIPv4(ip)
	if tag == "" || uid <= 0 || ip == "" {
		return
	}
	lastUserIP.Store(tag+"|"+strconv.Itoa(uid), ip)
}

func LastUserIP(tag string, uid int) string {
	if tag == "" || uid <= 0 {
		return ""
	}
	if value, ok := lastUserIP.Load(tag + "|" + strconv.Itoa(uid)); ok {
		if ip, ok := value.(string); ok {
			return ip
		}
	}
	return ""
}

func Snapshot(tag string, reset bool) []Stat {
	result := make([]Stat, 0)
	stats.Range(func(_, value any) bool {
		stat := value.(*usageStat)
		if stat.tag != tag {
			return true
		}

		var connections, upload, download int64
		if reset {
			connections = stat.connections.Swap(0)
			upload = stat.traffic.UpCounter.Swap(0)
			download = stat.traffic.DownCounter.Swap(0)
		} else {
			connections = stat.connections.Load()
			upload = stat.traffic.UpCounter.Load()
			download = stat.traffic.DownCounter.Load()
		}

		if connections > 0 || upload > 0 || download > 0 {
			result = append(result, Stat{
				UID:         stat.uid,
				IP:          stat.ip,
				Connections: connections,
				Upload:      upload,
				Download:    download,
			})
		}

		return true
	})
	return result
}

func getStat(tag string, uid int, ip string) *usageStat {
	ip = normalizeIPv4(ip)
	if tag == "" || uid <= 0 || ip == "" {
		return nil
	}

	key := usageKey{Tag: tag, UID: uid, IP: ip}
	if value, ok := stats.Load(key); ok {
		return value.(*usageStat)
	}

	stat := &usageStat{
		tag:     tag,
		uid:     uid,
		ip:      ip,
		traffic: &counter.TrafficStorage{},
	}
	value, _ := stats.LoadOrStore(key, stat)
	return value.(*usageStat)
}

func normalizeIPv4(ip string) string {
	ip = strings.TrimSpace(strings.TrimPrefix(ip, "::ffff:"))
	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return ""
	}
	v4 := parsed.To4()
	if v4 == nil {
		return ""
	}
	return v4.String()
}
