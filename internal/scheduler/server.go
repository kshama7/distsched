// Package scheduler implements the distsched scheduler node: the gRPC
// JobService and WorkerService, a BoltDB-backed durable store, and an in-memory
// priority queue. Milestone 2 added persistence, priority queueing, and worker
// registration + heartbeat; Milestone 3 adds task dispatch via leases,
// exponential-backoff retry, and a dead-letter queue. Clustering (Milestone 4)
// builds on this.
package scheduler

import (
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	// LeaseTTL is how long a dispatched task may run before its lease is
	// considered lost. Reaping of expired leases arrives in Milestone 5; for now
	// the deadline is recorded and handed to the worker.
	LeaseTTL time.Duration
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
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 30 * time.Second
	}
}

// lease tracks one in-flight task attempt handed to a worker.
type lease struct {
	taskID   string
	jobID    string
	workerID string
	attempt  int32
	deadline time.Time
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

	// workersMu guards the in-memory worker liveness cache.
	workersMu sync.Mutex
	workers   map[string]*distschedv1.WorkerInfo

	// dispatchMu guards in-flight leases and pending retry/delay timers.
	dispatchMu  sync.Mutex
	leases      map[string]*lease      // task_id -> lease
	retryTimers map[string]*time.Timer // job_id -> pending enqueue timer

	closed atomic.Bool
	grpc   *grpc.Server
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
		cfg:         cfg,
		log:         log.With("node_id", cfg.NodeID),
		store:       st,
		queue:       queue.New(),
		workers:     make(map[string]*distschedv1.WorkerInfo),
		leases:      make(map[string]*lease),
		retryTimers: make(map[string]*time.Timer),
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

// recover rebuilds in-memory state from the durable store. QUEUED jobs whose
// scheduled time has passed are enqueued immediately; those scheduled in the
// future (e.g. a retry mid-backoff at crash time) are re-armed with a timer.
// Jobs left RUNNING by a crash are orphaned here and reclaimed in Milestone 5.
func (s *Server) recover() error {
	jobs, err := s.store.ListJobs()
	if err != nil {
		return fmt.Errorf("recover jobs: %w", err)
	}
	now := time.Now()
	var requeued, delayed, orphaned int
	for _, j := range jobs {
		switch j.GetState() {
		case distschedv1.JobState_JOB_STATE_QUEUED:
			at := j.GetScheduledAt().AsTime()
			if j.GetScheduledAt() == nil || !at.After(now) {
				s.queue.Push(j.GetId(), j.GetPriority(), j.GetCreatedAt().AsTime())
				requeued++
			} else {
				s.armEnqueue(j.GetId(), at.Sub(now))
				delayed++
			}
		case distschedv1.JobState_JOB_STATE_RUNNING:
			orphaned++
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
		"delayed", delayed,
		"orphaned_running", orphaned,
		"workers", len(workers))
	return nil
}

// armEnqueue schedules a job to enter the ready queue after delay. A non-positive
// delay enqueues immediately. Used for retry backoff and recovery of delayed jobs.
func (s *Server) armEnqueue(jobID string, delay time.Duration) {
	if delay <= 0 {
		s.enqueueJob(jobID)
		return
	}
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	if t, ok := s.retryTimers[jobID]; ok {
		t.Stop()
	}
	s.retryTimers[jobID] = time.AfterFunc(delay, func() {
		s.dispatchMu.Lock()
		delete(s.retryTimers, jobID)
		s.dispatchMu.Unlock()
		s.enqueueJob(jobID)
	})
}

// enqueueJob loads a job and pushes it onto the ready queue if it is still
// QUEUED (i.e. not canceled in the meantime).
func (s *Server) enqueueJob(jobID string) {
	if s.closed.Load() {
		return
	}
	job, err := s.store.GetJob(jobID)
	if err != nil {
		s.log.Warn("enqueue: load job", "job_id", jobID, "err", err)
		return
	}
	if job.GetState() != distschedv1.JobState_JOB_STATE_QUEUED {
		return
	}
	if s.queue.Push(job.GetId(), job.GetPriority(), job.GetCreatedAt().AsTime()) {
		s.log.Info("job ready", "job_id", job.GetId(), "attempt", job.GetAttempt(), "queue_depth", s.queue.Len())
	}
}

// Serve serves the gRPC services on lis until Shutdown.
func (s *Server) Serve(lis net.Listener) error {
	s.log.Info("scheduler serving", "addr", lis.Addr().String())
	return s.grpc.Serve(lis)
}

// Shutdown stops timers, gracefully stops the gRPC server, and closes the store.
func (s *Server) Shutdown() {
	s.closed.Store(true)
	s.dispatchMu.Lock()
	for _, t := range s.retryTimers {
		t.Stop()
	}
	s.retryTimers = make(map[string]*time.Timer)
	s.dispatchMu.Unlock()

	if s.grpc != nil {
		s.grpc.GracefulStop()
	}
	if err := s.store.Close(); err != nil {
		s.log.Error("closing store", "err", err)
	}
}

// QueueDepth returns the number of ready jobs; used by tests and metrics.
func (s *Server) QueueDepth() int { return s.queue.Len() }

// InFlight returns the number of outstanding leases; used by tests and metrics.
func (s *Server) InFlight() int {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	return len(s.leases)
}

// DeadLetterCount returns the number of dead-lettered jobs.
func (s *Server) DeadLetterCount() int {
	n, err := s.store.CountDeadLetter()
	if err != nil {
		s.log.Error("count dead-letter", "err", err)
	}
	return n
}
