package doctor

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"time"
)

func Check(out io.Writer) bool {
	ok := true
	report := func(level, key, value string) {
		fmt.Fprintf(out, "%s %-20s %s\n", level, key, value)
		if level == "ERROR" {
			ok = false
		}
	}
	if runtime.GOOS != "linux" {
		report("ERROR", "OS", "Linux required for /proc and cgroups")
	} else {
		report("OK", "OS", runtime.GOOS+"/"+runtime.GOARCH)
	}
	for _, p := range []struct {
		name     string
		args     []string
		required bool
	}{{"docker", []string{"version", "--format", "{{.Server.Version}}"}, true}, {"docker", []string{"compose", "version"}, true}, {"python3", []string{"--version"}, true}, {"go", []string{"version"}, false}, {"uname", []string{"-r"}, true}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		b, e := exec.CommandContext(ctx, p.name, p.args...).CombinedOutput()
		cancel()
		if e != nil {
			level := "WARNING"
			if p.required {
				level = "ERROR"
			}
			report(level, p.name, "unavailable; install/start it: "+e.Error())
		} else {
			report("OK", p.name, strings.TrimSpace(string(b)))
		}
	}
	var r syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &r) == nil {
		level := "OK"
		if r.Cur < 100000 {
			level = "WARNING"
		}
		report(level, "ulimit -n", fmt.Sprintf("%d; plan >= twice total clients plus broker sockets", r.Cur))
	}
	var fs syscall.Statfs_t
	if syscall.Statfs(".", &fs) == nil {
		report("OK", "free disk", fmt.Sprintf("%d bytes", fs.Bavail*uint64(fs.Bsize)))
	}
	for _, v := range []struct{ key, path string }{{"RAM", "/proc/meminfo"}, {"CPU governor", "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"}, {"swap", "/proc/swaps"}} {
		b, e := os.ReadFile(v.path)
		if e != nil {
			report("WARNING", v.key, "unavailable")
		} else {
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			level, advice := "OK", ""
			if v.key == "CPU governor" && strings.TrimSpace(string(b)) != "performance" {
				level, advice = "WARNING", "; frequency scaling is a controlled variable; record and hold this policy constant"
			}
			if v.key == "swap" && len(lines) > 1 {
				level, advice = "WARNING", "; swapping can affect tail latency; monitor usage and keep configuration constant"
			}
			report(level, v.key, strings.Join(lines[:min(len(lines), 2)], "; ")+advice)
		}
	}
	if _, e := os.Stat("/sys/fs/cgroup/cgroup.controllers"); e == nil {
		report("OK", "cgroup", "v2")
	} else {
		report("WARNING", "cgroup", "v2 unavailable; exact container counters may be missing")
	}
	report("OK", "logical CPUs", fmt.Sprint(runtime.NumCPU()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := exec.CommandContext(ctx, ".venv/bin/python", "-c", "import broker_analysis, jsonschema, numpy, matplotlib, pyarrow").CombinedOutput()
	cancel()
	if err != nil {
		report("ERROR", "analysis environment", "run make bootstrap from the repository root: "+err.Error())
	} else {
		report("OK", "analysis environment", "pinned Python analysis modules import successfully")
	}
	for _, port := range []string{"4222", "8222", "1883", "5672", "15672", "9092"} {
		l, e := net.Listen("tcp", "127.0.0.1:"+port)
		if e != nil {
			report("WARNING", "port "+port, "not available; stop conflicting instance before deployment")
		} else {
			_ = l.Close()
			report("OK", "port "+port, "available")
		}
	}
	return ok
}
