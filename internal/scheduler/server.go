// Package scheduler implements the distsched scheduler node: the gRPC
// JobService and WorkerService, a BoltDB-backed durable store, and an in-memory
// priority queue. Milestone 2 covers a single scheduler with persistence,
// priority queueing, and worker registration + heartbeat. Task dispatch
// (Milestone 3) and clustering (Milestone 4) build on this.
package scheduler

import (
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/queue"
	"github.com/kshama7/distsched/internal/store"
)

// Config configures a scheduler node.
type Config struct {
	// NodeID identifies this scheduler in the cluster.
	NodeID string
	// ListenAddr is the gRPC bind address (host:port).
	ListenAddr string
	// DataDir holds the BoltDB file.
	DataDir string
	// HeartbeatTimeout is how long a worker may go without a heartbeat before it
	// is eligible to be marked dead. Wired into eviction in Milestone 5.
	HeartbeatTimeout time.Duration
}

func (c *Config) withDefaults() {
	if c.NodeID == "" {
		c.NodeID = "scheduler-1"
	}
	if c.ListenAddr == "" {
		c.ListenAddr = ":7070"
	}
	if c.DataDir == "" {
		c.DataDir = "./data"
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 15 * time.Second
	}
}

// Server is a single scheduler node. It implements JobService and
// WorkerService.
type Server struct {
	distschedv1.UnimplementedJobServiceServer
	distschedv1.UnimplementedWorkerServiceServer

	cfg   Config
	log   *slog.Logger
	store *store.Store
	queue *queue.PriorityQueue

	// mu guards the in-memory worker liveness cache.
	mu      sync.Mutex
	workers map[string]*distschedv1.WorkerInfo

	grpc *grpc.Server
}

// New creates a scheduler, opening its store and recovering durable state into
// the in-memory queue and worker cache.
func New(cfg Config, log *slog.Logger) (*Server, error) {
	cfg.withDefaults()
	st, err := store.Open(filepath.Join(cfg.DataDir, "distsched.db"))
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:     cfg,
		log:     log.With("node_id", cfg.NodeID),
		store:   st,
		queue:   queue.New(),
		workers: make(map[string]*distschedv1.WorkerInfo),
	}
	if err := s.recover(); err != nil {
		_ = st.Close()
		return nil, err
	}

	// Build and register the gRPC server synchronously so Serve and Shutdown
	// (which may run on different goroutines) never race on s.grpc.
	s.grpc = grpc.NewServer()
	distschedv1.RegisterJobServiceServer(s.grpc, s)
	distschedv1.RegisterWorkerServiceServer(s.grpc, s)
	return s, nil
}

// recover rebuilds the priority queue from persisted QUEUED jobs and reloads
// the worker cache. This is the checkpoint-recovery path: the in-memory state
// is always reconstructable from the store.
func (s *Server) recover() error {
	jobs, err := s.store.ListJobs()
	if err != nil {
		return fmt.Errorf("recover jobs: %w", err)
	}
	requeued := 0
	for _, j := range jobs {
		if j.GetState() == distschedv1.JobState_JOB_STATE_QUEUED {
			s.queue.Push(j.GetId(), j.GetPriority(), j.GetCreatedAt().AsTime())
			requeued++
		}
	}
	workers, err := s.store.ListWorkers()
	if err != nil {
		return fmt.Errorf("recover workers: %w", err)
	}
	for _, w := range workers {
		s.workers[w.GetId()] = w
	}
	s.log.Info("recovered state",
		"total_jobs", len(jobs),
		"requeued", requeued,
		"workers", len(workers))
	return nil
}

// Serve serves the gRPC services on lis until Shutdown.
func (s *Server) Serve(lis net.Listener) error {
	s.log.Info("scheduler serving", "addr", lis.Addr().String())
	return s.grpc.Serve(lis)
}

// Shutdown gracefully stops the gRPC server and closes the store.
func (s *Server) Shutdown() {
	if s.grpc != nil {
		s.grpc.GracefulStop()
	}
	if err := s.store.Close(); err != nil {
		s.log.Error("closing store", "err", err)
	}
}

// QueueDepth returns the number of ready jobs; used by tests and metrics.
func (s *Server) QueueDepth() int { return s.queue.Len() }
