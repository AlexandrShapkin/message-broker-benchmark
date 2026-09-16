package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/message-broker-benchmark/message-broker-benchmark/drivers/kafka"
	"github.com/message-broker-benchmark/message-broker-benchmark/drivers/mosquitto"
	"github.com/message-broker-benchmark/message-broker-benchmark/drivers/nats"
	"github.com/message-broker-benchmark/message-broker-benchmark/drivers/rabbitmq"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/deployment"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/doctor"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/driver"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/runner"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

var registry = driver.Registry{"nats": nats.New, "mosquitto": mosquitto.New, "rabbitmq": rabbitmq.New, "kafka": kafka.New}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx, os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: bench doctor | validate FILE | run --scenario FILE | matrix --scenario FILE | smoke --all | summarize RUN | provision/cleanup --broker NAME | self-test")
	}
	root, e := runner.Root()
	if e != nil {
		return e
	}
	switch args[0] {
	case "doctor":
		if !doctor.Check(os.Stdout) {
			return fmt.Errorf("doctor found errors")
		}
		return nil
	case "validate":
		if len(args) != 2 {
			return fmt.Errorf("validate requires one YAML path")
		}
		s, e := scenario.Load(args[1])
		if e != nil {
			return e
		}
		expanded, e := s.Expand()
		if e != nil {
			return e
		}
		for _, s := range expanded {
			if _, e = registry.New(s, "validation"); e != nil {
				return e
			}
		}
		fmt.Println("OK", args[1])
		return nil
	case "summarize":
		if len(args) != 2 {
			return fmt.Errorf("summarize requires run path or run_id")
		}
		path := args[1]
		if _, e = os.Stat(path); e != nil {
			path = filepath.Join(root, "results", path)
		}
		cmd := exec.CommandContext(ctx, filepath.Join(root, ".venv/bin/python"), "-m", "broker_analysis", "--run", path, "--output", filepath.Join(root, "analysis/output"))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	case "self-test":
		return selfTest(ctx, args[1:], root)
	case "reproduce":
		if len(args) != 2 {
			return fmt.Errorf("reproduce requires a saved run directory")
		}
		return reproduce(ctx, args[1:], root)
	case "run", "matrix", "smoke", "provision", "cleanup":
		f := flag.NewFlagSet(args[0], flag.ContinueOnError)
		path := f.String("scenario", "", "scenario YAML")
		broker := f.String("broker", "", "broker override")
		digest := f.String("image-digest", "", "immutable sha256 digest override")
		all := f.Bool("all", false, "all brokers")
		results := f.String("results", filepath.Join(root, "results"), "raw output root")
		if e = f.Parse(args[1:]); e != nil {
			return e
		}
		if f.NArg() != 0 {
			return fmt.Errorf("unexpected arguments: %v", f.Args())
		}
		if args[0] == "provision" || args[0] == "cleanup" {
			s, e := scenario.Load(filepath.Join(root, "scenarios/smoke/nats.yaml"))
			if e != nil {
				return e
			}
			s.SUT.Broker = *broker
			if _, e = registry.New(s, "provision"); e != nil {
				return e
			}
			release, e := deployment.Acquire(root, s.SUT.Broker)
			if e != nil {
				return e
			}
			defer release()
			d := deployment.New(root, s)
			c, cancel := context.WithTimeout(ctx, 180*time.Second)
			defer cancel()
			if args[0] == "cleanup" {
				d.Owned = true
				return d.Stop(c, "clean")
			}
			if e = d.Start(c, "reuse"); e != nil {
				return e
			}
			return d.Wait(c)
		}
		var scenarios []scenario.Scenario
		if args[0] == "smoke" {
			brokers := []string{*broker}
			if *all {
				brokers = scenario.Brokers()
			}
			for _, b := range brokers {
				if b == "" {
					return fmt.Errorf("smoke requires --broker or --all")
				}
				s, e := scenario.Load(filepath.Join(root, "scenarios/smoke", b+".yaml"))
				if e != nil {
					return e
				}
				scenarios = append(scenarios, s)
			}
		} else {
			if *path == "" {
				return fmt.Errorf("--scenario is required")
			}
			s, e := scenario.Load(*path)
			if e != nil {
				return e
			}
			if *broker != "" {
				s.SUT.Broker = *broker
			}
			if *digest != "" {
				s.SUT.ImageDigest = *digest
			}
			if args[0] == "matrix" {
				scenarios, e = s.Expand()
				if e != nil {
					return e
				}
			} else {
				if s.Matrix.Axis != "" {
					return fmt.Errorf("scenario contains a matrix; use bench matrix")
				}
				scenarios = []scenario.Scenario{s}
			}
		}
		var combined error
		var smokeRuns []string
		for _, s := range scenarios {
			for rep := 1; rep <= s.Repetitions; rep++ {
				if ctx.Err() != nil {
					return errors.Join(combined, ctx.Err())
				}
				dir, e := runner.Run(ctx, root, *results, s, registry, rep)
				_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"run": dir, "broker": s.SUT.Broker, "repetition": rep, "success": e == nil})
				if e != nil {
					combined = errors.Join(combined, fmt.Errorf("%s: %w", s.SUT.Broker, e))
					continue
				}
				if args[0] == "smoke" {
					smokeRuns = append(smokeRuns, dir)
					b, err := os.ReadFile(filepath.Join(dir, "correctness/summary.json"))
					if err != nil {
						combined = errors.Join(combined, err)
						continue
					}
					var c map[string]uint64
					if err = json.Unmarshal(b, &c); err != nil {
						combined = errors.Join(combined, err)
						continue
					}
					if c["delivered"] == 0 || c["lost"] > 0 || c["errors"] > 0 || c["unexpected"] > 0 {
						combined = errors.Join(combined, fmt.Errorf("%s smoke correctness failed: %+v", s.SUT.Broker, c))
					}
				}
			}
		}
		if args[0] == "smoke" && combined == nil && len(smokeRuns) > 0 {
			analysisArgs := []string{"-m", "broker_analysis.smoke", "--output", filepath.Join(root, "analysis/smoke-output", filepath.Base(smokeRuns[0]))}
			for _, dir := range smokeRuns {
				analysisArgs = append(analysisArgs, "--run", dir)
			}
			cmd := exec.CommandContext(ctx, filepath.Join(root, ".venv/bin/python"), analysisArgs...)
			cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
			combined = cmd.Run()
		}
		return combined
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
