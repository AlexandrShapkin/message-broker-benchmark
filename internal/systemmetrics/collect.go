// Package systemmetrics keeps OS/container measurement outside broker drivers.
package systemmetrics

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func num(s string) uint64 { v, _ := strconv.ParseUint(s, 10, 64); return v }
func Process() map[string]any {
	m := map[string]any{"schema_version": 1, "scope": "generator_process", "time": time.Now().UTC().Format(time.RFC3339Nano)}
	b, e := os.ReadFile("/proc/self/stat")
	if e != nil {
		m["error"] = e.Error()
		return m
	}
	fields := strings.Fields(string(b)[strings.LastIndex(string(b), ")")+2:])
	if len(fields) > 21 {
		m["cpu_ticks"] = num(fields[11]) + num(fields[12])
		m["threads"] = num(fields[17])
		m["rss_bytes"] = num(fields[21]) * uint64(os.Getpagesize())
	}
	fds, e := os.ReadDir("/proc/self/fd")
	if e == nil {
		m["file_descriptors"] = len(fds)
		sockets := 0
		for _, f := range fds {
			target, _ := os.Readlink("/proc/self/fd/" + f.Name())
			if strings.HasPrefix(target, "socket:[") {
				sockets++
			}
		}
		m["sockets"] = sockets
	}
	b, e = os.ReadFile("/proc/self/io")
	if e == nil {
		for _, line := range strings.Split(string(b), "\n") {
			f := strings.Fields(line)
			if len(f) == 2 {
				m[strings.TrimSuffix(f[0], ":")] = num(f[1])
			}
		}
	}
	b, e = os.ReadFile("/proc/net/dev")
	if e == nil {
		var rx, tx uint64
		for _, l := range strings.Split(string(b), "\n") {
			p := strings.SplitN(l, ":", 2)
			if len(p) != 2 {
				continue
			}
			f := strings.Fields(p[1])
			if len(f) >= 16 {
				rx += num(f[0])
				tx += num(f[8])
			}
		}
		m["host_network_rx_bytes"] = rx
		m["host_network_tx_bytes"] = tx
	}
	return m
}
func Container(ctx context.Context, id string) map[string]any {
	m := map[string]any{"schema_version": 1, "scope": "broker_container", "time": time.Now().UTC().Format(time.RFC3339Nano)}
	b, e := exec.CommandContext(ctx, "docker", "stats", "--no-stream", "--format", "{{json .}}", id).Output()
	if e != nil {
		m["error"] = e.Error()
		return m
	}
	var stats map[string]any
	if e = json.Unmarshal(b, &stats); e != nil {
		m["error"] = e.Error()
		return m
	}
	m["docker_stats"] = stats
	b, e = exec.CommandContext(ctx, "docker", "inspect", "--format", "{{json .State}}", id).Output()
	if e == nil {
		var state map[string]any
		if json.Unmarshal(b, &state) == nil {
			m["oom_killed"] = state["OOMKilled"]
			m["running"] = state["Running"]
			m["host_pid"] = state["Pid"]
		}
	}
	return m
}

// ProcContainer collects exact cumulative cgroup/proc counters inside the
// container; missing kernel files remain explicitly unavailable.
func ProcContainer(ctx context.Context, id string, processName ...string) map[string]any {
	m := map[string]any{"schema_version": 1, "scope": "broker_proc", "time": time.Now().UTC().Format(time.RFC3339Nano)}
	name := ""
	if len(processName) > 0 {
		name = processName[0]
	}
	script := `pid=1
if [ -n "$1" ]; then
  pid=2147483647
  for f in /proc/[0-9]*/comm; do
    IFS= read -r comm < "$f" 2>/dev/null || continue
    if [ "$comm" = "$1" ]; then
      candidate=${f#/proc/}; candidate=${candidate%/comm}
      [ "$candidate" -lt "$pid" ] && pid=$candidate
    fi
  done
  [ "$pid" -ne 2147483647 ] || exit 1
fi
echo FILE:broker_pid; echo "$pid"
for f in /proc/$pid/status /proc/$pid/io /proc/net/dev /proc/net/tcp /proc/net/tcp6 /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.stat /sys/fs/cgroup/io.stat; do
  data=$(cat "$f" 2>/dev/null) && { echo "FILE:$f"; printf '%s\n' "$data"; }
done
fds=$(ls /proc/$pid/fd 2>/dev/null) && { echo FILE:fd_count; printf '%s\n' "$fds" | wc -l; }
true`
	b, e := exec.CommandContext(ctx, "docker", "exec", id, "sh", "-c", script, "bench-collector", name).Output()
	if e != nil {
		m["error"] = e.Error()
		return m
	}
	parts := map[string]string{}
	key := ""
	var contents strings.Builder
	flush := func() {
		if key != "" {
			parts[key] = contents.String()
		}
		contents.Reset()
	}
	sc := bufio.NewScanner(strings.NewReader(string(b)))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "FILE:") {
			flush()
			key = strings.TrimPrefix(line, "FILE:")
		} else {
			contents.WriteString(line)
			contents.WriteByte('\n')
		}
	}
	flush()
	if err := sc.Err(); err != nil {
		m["error"] = err.Error()
	}
	m["files"] = parts
	return m
}
func NATS(ctx context.Context) map[string]any {
	m := map[string]any{"schema_version": 1, "scope": "nats_varz", "time": time.Now().UTC().Format(time.RFC3339Nano)}
	req, e := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:8222/varz", nil)
	if e != nil {
		m["error"] = e.Error()
		return m
	}
	r, e := http.DefaultClient.Do(req)
	if e != nil {
		m["error"] = e.Error()
		return m
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		m["error"] = fmt.Sprintf("HTTP %d", r.StatusCode)
		return m
	}
	var v map[string]any
	if e = json.NewDecoder(r.Body).Decode(&v); e != nil {
		m["error"] = e.Error()
	} else {
		for _, k := range []string{"connections", "total_connections", "in_msgs", "out_msgs", "in_bytes", "out_bytes", "slow_consumers", "mem", "cpu", "subscriptions", "version"} {
			m[k] = v[k]
		}
	}
	return m
}

// BrokerConnections counts established server-side protocol TCP endpoints only.
// Health probes can briefly contribute one connection; management ports excluded.
func BrokerConnections(row map[string]any, broker string) (int, bool) {
	port, ok := map[string]int{"nats": 4222, "mosquitto": 1883, "rabbitmq": 5672, "kafka": 9092}[broker]
	if !ok {
		return 0, false
	}
	return ConnectionsOnPort(row, port)
}

// ConnectionsOnPort permits new protocols without changes to the runner.
func ConnectionsOnPort(row map[string]any, port int) (int, bool) {
	files, ok := row["files"].(map[string]string)
	if !ok || port <= 0 || port > 65535 {
		return 0, false
	}
	count := 0
	available := false
	for _, file := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data := files[file]
		if data != "" {
			available = true
		}
		for _, line := range strings.Split(data, "\n") {
			f := strings.Fields(line)
			if len(f) > 3 && f[3] == "01" {
				parts := strings.Split(f[1], ":")
				p, e := strconv.ParseInt(parts[len(parts)-1], 16, 32)
				if e == nil && int(p) == port {
					count++
				}
			}
		}
	}
	return count, available
}

// NativeCollectors contains optional native endpoints; all brokers additionally
// receive the common container and generator collectors.
var NativeCollectors = map[string]func(context.Context) map[string]any{
	"nats":     NATS,
	"rabbitmq": RabbitMQ,
}

func RabbitMQ(ctx context.Context) map[string]any {
	m := map[string]any{"schema_version": 1, "scope": "rabbitmq_overview", "time": time.Now().UTC().Format(time.RFC3339Nano)}
	req, e := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:15672/api/overview", nil)
	if e != nil {
		m["error"] = e.Error()
		return m
	}
	req.SetBasicAuth("guest", "guest") // public loopback-only test fixture
	r, e := http.DefaultClient.Do(req)
	if e != nil {
		m["error"] = e.Error()
		return m
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		m["error"] = fmt.Sprintf("HTTP %d", r.StatusCode)
		return m
	}
	var v map[string]any
	if e = json.NewDecoder(r.Body).Decode(&v); e != nil {
		m["error"] = e.Error()
	} else {
		for _, k := range []string{"rabbitmq_version", "object_totals", "queue_totals", "message_stats"} {
			m[k] = v[k]
		}
	}
	return m
}
