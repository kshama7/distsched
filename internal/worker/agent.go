// Package worker implements the worker agent: it discovers and follows the
// cluster leader, registers, heartbeats, and pulls tasks to execute as
// subprocesses, reporting each result. Capacity bounds concurrent tasks.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

// drainTimeout bounds how long Run waits for in-flight tasks on shutdown.
const drainTimeout = 30 * time.Second

// doneRetention is how long a finished task_id is remembered to suppress
// duplicate delivery of the same attempt.
const doneRetention = 10 * time.Minute

// Config configures a worker agent.
type Config struct {
	// Schedulers is the seed list of scheduler addresses used to discover the
	// current leader.
	Schedulers []string
	// Advertise is the address this worker advertises (informational).
	Advertise string
	// Capacity is the max number of concurrent tasks.
	Capacity int32
	// Labels are arbitrary key/value tags.
	Labels map[string]string
	// HeartbeatInterval is the gap between heartbeats.
	HeartbeatInterval time.Duration
	// PollInterval is the gap between task polls.
	PollInterval time.Duration
}

func (c *Config) withDefaults() {
	if c.Capacity <= 0 {
		c.Capacity = 4
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.PollInterval == 0 {
		c.PollInterval = time.Second
	}
}

// Agent is a worker process.
type Agent struct {
	cfg Config
	log *slog.Logger

	connMu     sync.Mutex
	conn       *grpc.ClientConn
	client     distschedv1.WorkerServiceClient
	leaderAddr string
	workerID   string

	mu      sync.Mutex
	running map[string]struct{}  // task_id set, currently executing
	done    map[string]time.Time // recently finished task_ids -> completion time
	wg      sync.WaitGroup
}

// New constructs an Agent.
func New(cfg Config, log *slog.Logger) *Agent {
	cfg.withDefaults()
	return &Agent{
		cfg:     cfg,
		log:     log,
		running: make(map[string]struct{}),
		done:    make(map[string]time.Time),
	}
}

// Run discovers the leader, registers, then heartbeats and polls for work until
// ctx is canceled, after which it drains in-flight tasks.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.bootstrap(ctx); err != nil {
		return err
	}
	defer a.closeConn()

	hbTicker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer hbTicker.Stop()
	pollTicker := time.NewTicker(a.cfg.PollInterval)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return a.drain()
		case <-hbTicker.C:
			a.heartbeat(ctx)
		case <-pollTicker.C:
			a.poll(ctx)
		}
	}
}

// bootstrap finds the leader, connects, and registers, retrying with backoff
// until it succeeds or ctx is canceled.
func (a *Agent) bootstrap(ctx context.Context) error {
	backoff := 300 * time.Millisecond
	for {
		if err := a.connectLeader(ctx); err == nil {
			if err := a.register(ctx); err == nil {
				return nil
			} else {
				a.log.Warn("register failed", "err", err)
			}
		} else {
			a.log.Warn("leader discovery failed; retrying", "err", err, "backoff", backoff.String())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// discoverLeader queries each seed's cluster status to find the leader address.
func (a *Agent) discoverLeader(ctx context.Context) (string, error) {
	var lastErr error
	for _, seed := range a.cfg.Schedulers {
		conn, err := grpc.NewClient(seed, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			lastErr = err
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := distschedv1.NewSchedulerServiceClient(conn).GetClusterStatus(cctx, &distschedv1.GetClusterStatusRequest{})
		cancel()
		_ = conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		holder := resp.GetLease().GetHolderId()
		if holder == "" {
			continue // no leader yet
		}
		for _, n := range resp.GetSchedulers() {
			if n.GetId() == holder {
				return n.GetAddress(), nil
			}
		}
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("no leader elected among seeds %v", a.cfg.Schedulers)
}

// connectLeader resolves the leader and dials it, replacing any prior connection.
func (a *Agent) connectLeader(ctx context.Context) error {
	addr, err := a.discoverLeader(ctx)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	a.connMu.Lock()
	old := a.conn
	a.conn = conn
	a.client = distschedv1.NewWorkerServiceClient(conn)
	a.leaderAddr = addr
	a.connMu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	a.log.Info("connected to leader", "addr", addr)
	return nil
}

func (a *Agent) closeConn() {
	a.connMu.Lock()
	defer a.connMu.Unlock()
	if a.conn != nil {
		_ = a.conn.Close()
		a.conn = nil
	}
}

func (a *Agent) workerClient() distschedv1.WorkerServiceClient {
	a.connMu.Lock()
	defer a.connMu.Unlock()
	return a.client
}

func (a *Agent) currentWorkerID() string {
	a.connMu.Lock()
	defer a.connMu.Unlock()
	return a.workerID
}

// register registers with the currently connected leader. It reuses any
// previously assigned worker ID so a worker keeps one identity across leader
// failovers and re-registrations (rather than accumulating stale entries).
func (a *Agent) register(ctx context.Context) error {
	client := a.workerClient()
	resp, err := client.RegisterWorker(ctx, &distschedv1.RegisterWorkerRequest{
		Worker: &distschedv1.WorkerInfo{
			Id:       a.currentWorkerID(),
			Address:  a.cfg.Advertise,
			Capacity: a.cfg.Capacity,
			Labels:   a.cfg.Labels,
		},
	})
	if err != nil {
		return err
	}
	a.connMu.Lock()
	a.workerID = resp.GetWorkerId()
	a.connMu.Unlock()
	a.log.Info("registered with leader", "worker_id", resp.GetWorkerId(), "leader", resp.GetLeaderAddress())
	return nil
}

// onLeadershipError reconnects to the new leader when an RPC indicates this node
// is no longer the leader (or is unreachable). Returns true if it handled the
// error.
func (a *Agent) onLeadershipError(ctx context.Context, err error) bool {
	code := status.Code(err)
	if code != codes.FailedPrecondition && code != codes.Unavailable {
		return false
	}
	a.log.Warn("leader changed or unreachable; rediscovering", "err", err)
	if berr := a.bootstrap(ctx); berr != nil {
		a.log.Error("re-bootstrap failed", "err", berr)
	}
	return true
}

// heartbeat sends one heartbeat with the current in-flight task IDs.
func (a *Agent) heartbeat(ctx context.Context) {
	resp, err := a.workerClient().Heartbeat(ctx, &distschedv1.HeartbeatRequest{
		WorkerId:       a.currentWorkerID(),
		RunningTaskIds: a.runningTaskIDs(),
	})
	if err != nil {
		if !a.onLeadershipError(ctx, err) {
			a.log.Warn("heartbeat failed", "err", err)
		}
		return
	}
	if !resp.GetOk() {
		// Leader no longer knows this worker (e.g. after its restart); re-register.
		a.log.Warn("leader does not recognize worker; re-registering")
		if err := a.register(ctx); err != nil {
			a.log.Error("re-register failed", "err", err)
		}
	}
}

// poll requests up to the free-capacity number of tasks and launches each.
func (a *Agent) poll(ctx context.Context) {
	free := a.freeCapacity()
	if free <= 0 {
		return
	}
	resp, err := a.workerClient().PollTask(ctx, &distschedv1.PollTaskRequest{
		WorkerId: a.currentWorkerID(),
		MaxTasks: int32(free),
	})
	if err != nil {
		if !a.onLeadershipError(ctx, err) {
			a.log.Warn("poll failed", "err", err)
		}
		return
	}
	for _, task := range resp.GetTasks() {
		if !a.tryStart(task.GetTaskId()) {
			continue // duplicate delivery or at capacity
		}
		a.wg.Add(1)
		go a.run(task)
	}
}

// run executes one task and reports the result. Execution uses a background
// context so a shutdown signal lets in-flight tasks finish during drain.
func (a *Agent) run(task *distschedv1.TaskAssignment) {
	defer a.wg.Done()
	defer a.finish(task.GetTaskId())

	a.log.Info("task started", "task_id", task.GetTaskId(), "job_id", task.GetJobId(), "attempt", task.GetAttempt())
	result, output, errMsg := execute(context.Background(), task.GetSpec())
	a.report(task.GetTaskId(), result, output, errMsg)

	a.log.Info("task finished",
		"task_id", task.GetTaskId(), "job_id", task.GetJobId(), "result", result.String(), "error", errMsg)
}

// report sends a result, retrying a few times so a transient error does not drop
// the outcome. If the leader changed, it rediscovers and re-reports.
func (a *Agent) report(taskID string, result distschedv1.TaskResult, output, errMsg string) {
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := a.workerClient().ReportTask(ctx, &distschedv1.ReportTaskRequest{
			WorkerId: a.currentWorkerID(),
			TaskId:   taskID,
			Result:   result,
			Output:   output,
			Error:    errMsg,
		})
		cancel()
		if err == nil {
			return
		}
		a.log.Warn("report failed; retrying", "task_id", taskID, "err", err)
		if a.onLeadershipError(context.Background(), err) {
			continue
		}
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	a.log.Error("report permanently failed; task may be re-leased after lease expiry", "task_id", taskID)
}

// drain waits for in-flight tasks to finish, bounded by drainTimeout.
func (a *Agent) drain() error {
	a.log.Info("worker draining", "worker_id", a.currentWorkerID(), "inflight", a.inflight())
	done := make(chan struct{})
	go func() { a.wg.Wait(); close(done) }()
	select {
	case <-done:
		a.log.Info("worker stopped cleanly", "worker_id", a.currentWorkerID())
	case <-time.After(drainTimeout):
		a.log.Warn("drain timeout; abandoning in-flight tasks", "inflight", a.inflight())
	}
	return nil
}

func (a *Agent) freeCapacity() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return int(a.cfg.Capacity) - len(a.running)
}

func (a *Agent) inflight() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.running)
}

func (a *Agent) runningTaskIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.running))
	for id := range a.running {
		out = append(out, id)
	}
	return out
}

// tryStart claims a slot for taskID. It returns false if the task is already
// running, was recently completed (idempotent suppression of duplicate
// delivery), or the worker is at capacity.
func (a *Agent) tryStart(taskID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.running[taskID]; ok {
		return false
	}
	if _, ok := a.done[taskID]; ok {
		a.log.Debug("suppressing duplicate task delivery", "task_id", taskID)
		return false
	}
	if len(a.running) >= int(a.cfg.Capacity) {
		return false
	}
	a.running[taskID] = struct{}{}
	return true
}

func (a *Agent) finish(taskID string) {
	a.mu.Lock()
	delete(a.running, taskID)
	now := time.Now()
	a.done[taskID] = now
	// Opportunistically prune stale done entries.
	for id, t := range a.done {
		if now.Sub(t) > doneRetention {
			delete(a.done, id)
		}
	}
	a.mu.Unlock()
}
