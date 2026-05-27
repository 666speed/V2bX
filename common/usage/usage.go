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
	key         usageKey
	tag         string
	uid         int
	ip          string
	active      atomic.Int64
	connections atomic.Int64
	traffic     *counter.TrafficStorage
}

var (
	stats      sync.Map
	statsMu    sync.Mutex
	lastUserIP sync.Map
)

func RecordConnection(tag string, uid int, ip string) {
	_, release := RecordConnectionTraffic(tag, uid, ip)
	release()
}

func RecordConnectionTraffic(tag string, uid int, ip string) (*counter.TrafficStorage, func()) {
	stat := beginStat(tag, uid, ip)
	if stat == nil {
		return nil, func() {}
	}
	stat.connections.Add(1)
	lastUserIP.Store(tag+"|"+strconv.Itoa(uid), stat.ip)

	var once sync.Once
	return stat.traffic, func() {
		once.Do(func() {
			if stat.active.Add(-1) < 0 {
				stat.active.Store(0)
			}
		})
	}
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
		if reset {
			deleteIdleStat(stat)
		}

		return true
	})
	return result
}

func beginStat(tag string, uid int, ip string) *usageStat {
	ip = normalizeIPv4(ip)
	if tag == "" || uid <= 0 || ip == "" {
		return nil
	}

	key := usageKey{Tag: tag, UID: uid, IP: ip}
	statsMu.Lock()
	defer statsMu.Unlock()

	if value, ok := stats.Load(key); ok {
		stat := value.(*usageStat)
		stat.active.Add(1)
		return stat
	}

	stat := &usageStat{
		key:     key,
		tag:     tag,
		uid:     uid,
		ip:      ip,
		traffic: &counter.TrafficStorage{},
	}
	stat.active.Store(1)
	value, _ := stats.LoadOrStore(key, stat)
	loaded := value.(*usageStat)
	if loaded != stat {
		loaded.active.Add(1)
	}
	return loaded
}

func getStat(tag string, uid int, ip string) *usageStat {
	ip = normalizeIPv4(ip)
	if tag == "" || uid <= 0 || ip == "" {
		return nil
	}

	key := usageKey{Tag: tag, UID: uid, IP: ip}
	statsMu.Lock()
	defer statsMu.Unlock()

	if value, ok := stats.Load(key); ok {
		return value.(*usageStat)
	}

	stat := &usageStat{
		key:     key,
		tag:     tag,
		uid:     uid,
		ip:      ip,
		traffic: &counter.TrafficStorage{},
	}
	value, _ := stats.LoadOrStore(key, stat)
	return value.(*usageStat)
}

func deleteIdleStat(stat *usageStat) {
	if stat.active.Load() != 0 ||
		stat.connections.Load() != 0 ||
		stat.traffic.UpCounter.Load() != 0 ||
		stat.traffic.DownCounter.Load() != 0 {
		return
	}

	statsMu.Lock()
	defer statsMu.Unlock()
	if stat.active.Load() == 0 &&
		stat.connections.Load() == 0 &&
		stat.traffic.UpCounter.Load() == 0 &&
		stat.traffic.DownCounter.Load() == 0 {
		stats.Delete(stat.key)
	}
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
