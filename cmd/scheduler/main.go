// Command scheduler runs a distsched scheduler node: a gRPC server backed by a
// BoltDB store and an in-memory priority queue.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kshama7/distsched/internal/buildinfo"
	"github.com/kshama7/distsched/internal/logging"
	"github.com/kshama7/distsched/internal/scheduler"
)

func main() {
	var (
		showVersion = flag.Bool("version", false, "print version and exit")
		nodeID      = flag.String("node-id", "scheduler-1", "scheduler node ID")
		listen      = flag.String("listen", ":7070", "gRPC listen address")
		dataDir     = flag.String("data-dir", "./data", "directory for the BoltDB store")
		peers       = flag.String("peers", "", "cluster membership as id=addr,... (including self); empty for single-node")
		hbTimeout   = flag.Duration("heartbeat-timeout", 15*time.Second, "worker liveness timeout")
		logLevel    = flag.String("log-level", "info", "log level (debug|info|warn|error)")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("scheduler %s (%s)\n", buildinfo.Version, buildinfo.Commit)
		return
	}

	log := logging.New(*logLevel)

	members, err := parseMembers(*peers)
	if err != nil {
		log.Error("parse --peers", "err", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Error("create data dir", "err", err)
		os.Exit(1)
	}

	srv, err := scheduler.New(scheduler.Config{
		NodeID:           *nodeID,
		ListenAddr:       *listen,
		DataDir:          *dataDir,
		Members:          members,
		HeartbeatTimeout: *hbTimeout,
	}, log)
	if err != nil {
		log.Error("init scheduler", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "addr", *listen, "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	select {
	case <-ctx.Done():
		log.Info("signal received, shutting down")
		srv.Shutdown()
	case err := <-serveErr:
		if err != nil {
			log.Error("serve", "err", err)
			srv.Shutdown()
			os.Exit(1)
		}
	}
}

// parseMembers parses "id=addr,id=addr" into cluster members. An empty string
// yields nil (single-node mode).
func parseMembers(s string) ([]scheduler.Member, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var members []scheduler.Member
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("invalid member %q (want id=addr)", part)
		}
		members = append(members, scheduler.Member{ID: strings.TrimSpace(id), Address: strings.TrimSpace(addr)})
	}
	return members, nil
}
