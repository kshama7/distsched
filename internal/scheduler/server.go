// Package scheduler implements the distsched scheduler node: the gRPC
// JobService, WorkerService, and SchedulerService, backed by a BoltDB store and
// an in-memory priority queue. Through Milestone 3 it ran standalone; Milestone
// 4 adds lease-based leader election (internal/cluster), best-effort metadata
// replication, and leader failover. Only the leader dispatches work and accepts
// writes; followers redirect clients to the leader and apply replicated state.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/cluster"
	"github.com/kshama7/distsched/internal/queue"
	"github.com/kshama7/distsched/internal/store"
)

// Member identifies one scheduler replica in the cluster.
type Member struct {
	ID      string
	Address string
}

// Config configures a scheduler node.
type Config struct {
	// NodeID identifies this scheduler in the cluster.
	NodeID string
	// ListenAddr is the gRPC bind address (host:port).
	ListenAddr string
	// DataDir holds the BoltDB file.
	DataDir string
	// Members is the full cluster membership (including this node). Empty means a
	// single-node cluster consisting of just this node.
	Members []Member
	// HeartbeatTimeout is how long a worker may go without a heartbeat before it
	// is eligible to be marked dead. Wired into eviction in Milestone 5.
	HeartbeatTimeout time.Duration
	// LeaseTTL is how long a dispatched task may run before its lease is lost.
	LeaseTTL time.Duration
	// HeartbeatInterval is the leader's replication/heartbeat cadence.
	HeartbeatInterval time.Duration
	// ElectionTimeoutMin/Max bound the randomized election timeout.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// ReaperInterval is how often the leader scans for dead workers and expired
	// leases to requeue their tasks.
	ReaperInterval time.Duration
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
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 250 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 800 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 1600 * time.Millisecond
	}
	if c.ReaperInterval == 0 {
		c.ReaperInterval = 3 * time.Second
	}
	if len(c.Members) == 0 {
		c.Members = []Member{{ID: c.NodeID, Address: c.ListenAddr}}
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

// Server is a single scheduler node.
type Server struct {
	distschedv1.UnimplementedJobServiceServer
	distschedv1.UnimplementedWorkerServiceServer
	distschedv1.UnimplementedSchedulerServiceServer

	cfg      Config
	log      *slog.Logger
	store    *store.Store
	queue    *queue.PriorityQueue
	node     *cluster.Node
	members  map[string]string // id -> address
	selfAddr string
	xport    *peerTransport

	// workersMu guards the in-memory worker liveness cache.
	workersMu sync.Mutex
	workers   map[string]*distschedv1.WorkerInfo

	// dispatchMu guards in-flight leases and pending retry/delay timers.
	dispatchMu  sync.Mutex
	leases      map[string]*lease
	retryTimers map[string]*time.Timer

	// replMu guards the leader's replication log and per-follower ack cursors.
	replMu    sync.Mutex
	replLog   []*distschedv1.ReplicationEntry
	replSeq   int64
	peerAcked map[string]int64

	// depMu guards the reverse dependency index: dependency job ID -> set of
	// dependent job IDs waiting on it (DAG reverse edges).
	depMu    sync.Mutex
	revIndex map[string]map[string]struct{}

	closed   atomic.Bool
	bgCancel context.CancelFunc
	grpc     *grpc.Server
}

// New creates a scheduler, opens its store, wires up leader election, and starts
// background loops (election + replication). For a single-node cluster it
// assumes leadership synchronously, so it is immediately writable.
func New(cfg Config, log *slog.Logger) (*Server, error) {
	cfg.withDefaults()
	st, err := store.Open(filepath.Join(cfg.DataDir, "distsched.db"))
	if err != nil {
		return nil, err
	}

	members := make(map[string]string, len(cfg.Members))
	memberIDs := make([]string, 0, len(cfg.Members))
	for _, m := range cfg.Members {
		members[m.ID] = m.Address
		memberIDs = append(memberIDs, m.ID)
	}

	s := &Server{
		cfg:         cfg,
		log:         log.With("node_id", cfg.NodeID),
		store:       st,
		queue:       queue.New(),
		members:     members,
		selfAddr:    members[cfg.NodeID],
		workers:     make(map[string]*distschedv1.WorkerInfo),
		leases:      make(map[string]*lease),
		retryTimers: make(map[string]*time.Timer),
		peerAcked:   make(map[string]int64),
		revIndex:    make(map[string]map[string]struct{}),
	}
	s.xport = newPeerTransport(members, cfg.NodeID, s.log)
	s.node = cluster.New(cluster.Config{
		NodeID:             cfg.NodeID,
		Members:            memberIDs,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
	}, s.xport, &electionStore{store: st}, s.log, s.onBecomeLeader, s.onStepDown)

	if err := s.recover(); err != nil {
		_ = st.Close()
		return nil, err
	}

	s.grpc = grpc.NewServer()
	distschedv1.RegisterJobServiceServer(s.grpc, s)
	distschedv1.RegisterWorkerServiceServer(s.grpc, s)
	distschedv1.RegisterSchedulerServiceServer(s.grpc, s)

	bgCtx, cancel := context.WithCancel(context.Background())
	s.bgCancel = cancel
	s.node.Start(bgCtx) // single-node: becomes leader + fires onBecomeLeader synchronously
	go s.replicationLoop(bgCtx)
	go s.reaperLoop(bgCtx)
	return s, nil
}

// electionStore adapts the BoltDB store to cluster.StateStore.
type electionStore struct{ store *store.Store }

func (e *electionStore) LoadState() (int64, string, error) { return e.store.LoadElectionState() }
func (e *electionStore) SaveState(term int64, votedFor string) error {
	return e.store.SaveElectionState(term, votedFor)
}

// recover loads the worker cache from the store. The scheduling queue is built
// only when this node becomes leader (onBecomeLeader), since followers do not
// dispatch.
func (s *Server) recover() error {
	workers, err := s.store.ListWorkers()
	if err != nil {
		return fmt.Errorf("recover workers: %w", err)
	}
	for _, w := range workers {
		s.workers[w.GetId()] = w
	}
	s.log.Info("recovered worker cache", "workers", len(workers))
	return nil
}

// onBecomeLeader rebuilds the in-memory scheduling state from the durable store
// and seeds the replication snapshot so followers receive full state.
func (s *Server) onBecomeLeader(term int64) {
	s.log.Info("assuming leadership", "term", term)
	s.rebuildSchedulingState()
	s.seedReplication()
}

// onStepDown halts dispatching: it clears the queue, timers, and leases, and
// drops replication state.
func (s *Server) onStepDown(term int64) {
	s.log.Info("relinquished leadership", "term", term)
	s.clearSchedulingState()
}

// rebuildSchedulingState repopulates the priority queue from QUEUED jobs in the
// store (immediately, or via a timer for jobs scheduled in the future).
func (s *Server) rebuildSchedulingState() {
	s.clearSchedulingState()
	jobs, err := s.store.ListJobs()
	if err != nil {
		s.log.Error("rebuild: list jobs", "err", err)
		return
	}
	now := time.Now()
	var requeued, delayed, orphaned int
	for _, j := range jobs {
		// Rebuild the reverse dependency index for every non-terminal job.
		if !terminal(j.GetState()) {
			s.addDeps(j)
		}
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
			// Checkpoint recovery: a job left RUNNING was orphaned by a crashed or
			// superseded leader (this node holds no lease for it). Treat the lost
			// attempt as a failure so it is retried (or dead-lettered if exhausted).
			if err := s.handleFailure(j, "reclaimed orphaned task after leadership change", now); err != nil {
				s.log.Error("reclaim orphaned job", "job_id", j.GetId(), "err", err)
			}
			orphaned++
		}
	}
	// Re-evaluate PENDING jobs: their dependencies may have completed before this
	// node took leadership.
	for _, j := range jobs {
		if j.GetState() == distschedv1.JobState_JOB_STATE_PENDING {
			s.tryPromote(j.GetId())
		}
	}
	s.log.Info("scheduling state rebuilt", "requeued", requeued, "delayed", delayed, "orphaned_reclaimed", orphaned)
}

// clearSchedulingState empties the queue, stops timers, and drops leases and the
// dependency index.
func (s *Server) clearSchedulingState() {
	s.queue.Clear()
	s.dispatchMu.Lock()
	for _, t := range s.retryTimers {
		t.Stop()
	}
	s.retryTimers = make(map[string]*time.Timer)
	s.leases = make(map[string]*lease)
	s.dispatchMu.Unlock()

	s.depMu.Lock()
	s.revIndex = make(map[string]map[string]struct{})
	s.depMu.Unlock()
}

// armEnqueue schedules a job to enter the ready queue after delay. A non-positive
// delay enqueues immediately. Used for retry backoff and delayed jobs.
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

// enqueueJob pushes a job onto the ready queue if it is still QUEUED and this
// node is the leader.
func (s *Server) enqueueJob(jobID string) {
	if s.closed.Load() || !s.node.IsLeader() {
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

// putJob persists a job and, if this node is the leader, appends it to the
// replication log for shipment to followers.
func (s *Server) putJob(job *distschedv1.Job) error {
	if err := s.store.PutJob(job); err != nil {
		return err
	}
	clone := proto.Clone(job).(*distschedv1.Job)
	s.appendRepl(&distschedv1.ReplicationEntry{
		Mutation: &distschedv1.ReplicationEntry_UpsertJob{UpsertJob: clone},
	})
	return nil
}

// putWorker persists a worker, replicates it, and refreshes the cache.
func (s *Server) putWorker(w *distschedv1.WorkerInfo) error {
	if err := s.store.PutWorker(w); err != nil {
		return err
	}
	clone := proto.Clone(w).(*distschedv1.WorkerInfo)
	s.appendRepl(&distschedv1.ReplicationEntry{
		Mutation: &distschedv1.ReplicationEntry_UpsertWorker{UpsertWorker: clone},
	})
	s.workersMu.Lock()
	s.workers[w.GetId()] = w
	s.workersMu.Unlock()
	return nil
}

// appendRepl adds an entry to the replication log (leader only).
func (s *Server) appendRepl(e *distschedv1.ReplicationEntry) {
	if !s.node.IsLeader() {
		return
	}
	s.replMu.Lock()
	s.replSeq++
	e.Seq = s.replSeq
	s.replLog = append(s.replLog, e)
	s.replMu.Unlock()
}

// seedReplication resets the replication log to a full snapshot of current state
// so any follower (acked seq 0) converges to leader state.
func (s *Server) seedReplication() {
	jobs, _ := s.store.ListJobs()
	workers, _ := s.store.ListWorkers()
	s.replMu.Lock()
	defer s.replMu.Unlock()
	s.replLog = s.replLog[:0]
	s.replSeq = 0
	s.peerAcked = make(map[string]int64)
	for _, j := range jobs {
		s.replSeq++
		s.replLog = append(s.replLog, &distschedv1.ReplicationEntry{
			Seq:      s.replSeq,
			Mutation: &distschedv1.ReplicationEntry_UpsertJob{UpsertJob: proto.Clone(j).(*distschedv1.Job)},
		})
	}
	for _, w := range workers {
		s.replSeq++
		s.replLog = append(s.replLog, &distschedv1.ReplicationEntry{
			Seq:      s.replSeq,
			Mutation: &distschedv1.ReplicationEntry_UpsertWorker{UpsertWorker: proto.Clone(w).(*distschedv1.WorkerInfo)},
		})
	}
}

// requireLeader returns a redirect error if this node is not the leader.
func (s *Server) requireLeader() error {
	if s.node.IsLeader() {
		return nil
	}
	return notLeaderError(s.leaderAddr())
}

func (s *Server) leaderAddr() string { return s.members[s.node.LeaderID()] }

// Serve serves the gRPC services on lis until Shutdown.
func (s *Server) Serve(lis net.Listener) error {
	s.log.Info("scheduler serving", "addr", lis.Addr().String(), "members", len(s.members))
	return s.grpc.Serve(lis)
}

// Shutdown stops background loops and timers, gracefully stops the gRPC server,
// and closes the store and peer connections.
func (s *Server) Shutdown() {
	s.closed.Store(true)
	if s.bgCancel != nil {
		s.bgCancel()
	}
	s.dispatchMu.Lock()
	for _, t := range s.retryTimers {
		t.Stop()
	}
	s.retryTimers = make(map[string]*time.Timer)
	s.dispatchMu.Unlock()

	if s.grpc != nil {
		s.grpc.GracefulStop()
	}
	s.xport.close()
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

// IsLeader reports whether this node is currently the cluster leader.
func (s *Server) IsLeader() bool { return s.node.IsLeader() }

// Term returns the current election term.
func (s *Server) Term() int64 { return s.node.Term() }
