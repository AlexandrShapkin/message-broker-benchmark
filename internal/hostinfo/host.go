package hostinfo

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func command(name string, args ...string) any {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	b, e := exec.CommandContext(ctx, name, args...).Output()
	if e != nil {
		return nil
	}
	return strings.TrimSpace(string(b))
}
func read(p string) any {
	b, e := os.ReadFile(p)
	if e != nil {
		return nil
	}
	return strings.TrimSpace(string(b))
}
func Collect(root string) map[string]any {
	hostname, _ := os.Hostname()
	var limit syscall.Rlimit
	_ = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit)
	m := map[string]any{"hostname": hostname, "os": runtime.GOOS, "architecture": runtime.GOARCH, "logical_cores": runtime.NumCPU(), "kernel": command("uname", "-r"), "cpu": command("lscpu", "-J"), "memory": read("/proc/meminfo"), "disk": command("lsblk", "-J", "-o", "NAME,TYPE,SIZE,ROTA,MODEL"), "filesystem": command("df", "-T", root), "nic": command("ip", "-j", "link"), "ulimit_nofile": limit.Cur, "cpu_governor": read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"), "swap": read("/proc/swaps"), "clock_ticks_per_second": command("getconf", "CLK_TCK"), "page_size": os.Getpagesize()}
	return m
}
func Source() map[string]any {
	status := command("git", "status", "--porcelain")
	var dirty any
	if status != nil {
		dirty = status != ""
	}
	return map[string]any{"commit": command("git", "rev-parse", "HEAD"), "dirty": dirty, "dirty_status_available": status != nil, "binary_version": "0.1.0", "go_version": runtime.Version(), "repository": "github.com/message-broker-benchmark/message-broker-benchmark", "pid": strconv.Itoa(os.Getpid())}
}
