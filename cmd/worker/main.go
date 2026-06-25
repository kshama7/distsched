// Command worker runs a distsched worker node: it registers with a scheduler
// and heartbeats. Task execution arrives in Milestone 3.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kshama7/distsched/internal/buildinfo"
	"github.com/kshama7/distsched/internal/logging"
	"github.com/kshama7/distsched/internal/worker"
)

func main() {
	var (
		showVersion = flag.Bool("version", false, "print version and exit")
		scheduler   = flag.String("scheduler", "localhost:7070", "scheduler gRPC address")
		advertise   = flag.String("advertise", "", "address this worker advertises (informational)")
		capacity     = flag.Int("capacity", 4, "max concurrent tasks")
		hbInterval   = flag.Duration("heartbeat-interval", 5*time.Second, "heartbeat interval")
		pollInterval = flag.Duration("poll-interval", time.Second, "task poll interval")
		logLevel    = flag.String("log-level", "info", "log level (debug|info|warn|error)")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("worker %s (%s)\n", buildinfo.Version, buildinfo.Commit)
		return
	}

	log := logging.New(*logLevel)

	agent := worker.New(worker.Config{
		SchedulerAddr:     *scheduler,
		Advertise:         *advertise,
		Capacity:          int32(*capacity),
		HeartbeatInterval: *hbInterval,
		PollInterval:      *pollInterval,
	}, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := agent.Run(ctx); err != nil {
		log.Error("worker exited with error", "err", err)
		os.Exit(1)
	}
}
