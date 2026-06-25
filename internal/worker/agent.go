// Package worker implements the worker agent: it registers with a scheduler,
// heartbeats, and pulls tasks to execute as subprocesses, reporting each
// result back. Capacity bounds the number of concurrent tasks.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

// drainTimeout bounds how long Run waits for in-flight tasks on shutdown.
const drainTimeout = 30 * time.Second

// Config configures a worker agent.
type Config struct {
	// SchedulerAddr is the scheduler gRPC address (host:port).
	SchedulerAddr string
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
	cfg      Config
	log      *slog.Logger
	client   distschedv1.WorkerServiceClient
	workerID string

	mu      sync.Mutex
	running map[string]struct{} // task_id set
	wg      sync.WaitGroup
}

// New constructs an Agent.
func New(cfg Config, log *slog.Logger) *Agent {
	cfg.withDefaults()
	return &Agent{cfg: cfg, log: log, running: make(map[string]struct{})}
}

// Run dials the scheduler, registers, then heartbeats and polls for work until
// ctx is canceled, after which it drains in-flight tasks.
func (a *Agent) Run(ctx context.Context) error {
	conn, err := grpc.NewClient(a.cfg.SchedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial scheduler: %w", err)
	}
	defer conn.Close()
	a.client = distschedv1.NewWorkerServiceClient(conn)

	if err := a.register(ctx); err != nil {
		return err
	}

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

// drain waits for in-flight tasks to finish, bounded by drainTimeout.
func (a *Agent) drain() error {
	a.log.Info("worker draining", "worker_id", a.workerID, "inflight", a.inflight())
	done := make(chan struct{})
	go func() { a.wg.Wait(); close(done) }()
	select {
	case <-done:
		a.log.Info("worker stopped cleanly", "worker_id", a.workerID)
	case <-time.After(drainTimeout):
		a.log.Warn("drain timeout; abandoning in-flight tasks", "inflight", a.inflight())
	}
	return nil
}

// register calls RegisterWorker, retrying until it succeeds or ctx is canceled.
func (a *Agent) register(ctx context.Context) error {
	backoff := 500 * time.Millisecond
	for {
		resp, err := a.client.RegisterWorker(ctx, &distschedv1.RegisterWorkerRequest{
			Worker: &distschedv1.WorkerInfo{
				Address:  a.cfg.Advertise,
				Capacity: a.cfg.Capacity,
				Labels:   a.cfg.Labels,
			},
		})
		if err == nil {
			a.workerID = resp.GetWorkerId()
			a.log.Info("registered with scheduler", "worker_id", a.workerID, "leader", resp.GetLeaderAddress())
			return nil
		}
		a.log.Warn("register failed; retrying", "err", err, "backoff", backoff.String())
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

// heartbeat sends one heartbeat with the current in-flight task IDs; if the
// scheduler no longer knows this worker it re-registers.
func (a *Agent) heartbeat(ctx context.Context) {
	resp, err := a.client.Heartbeat(ctx, &distschedv1.HeartbeatRequest{
		WorkerId:       a.workerID,
		RunningTaskIds: a.runningTaskIDs(),
	})
	if err != nil {
		a.log.Warn("heartbeat failed", "err", err)
		return
	}
	if !resp.GetOk() {
		a.log.Warn("scheduler does not recognize worker; re-registering")
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
	resp, err := a.client.PollTask(ctx, &distschedv1.PollTaskRequest{
		WorkerId: a.workerID,
		MaxTasks: int32(free),
	})
	if err != nil {
		a.log.Warn("poll failed", "err", err)
		return
	}
	for _, task := range resp.GetTasks() {
		if !a.tryStart(task.GetTaskId()) {
			// Already running this task (duplicate delivery) or at capacity.
			continue
		}
		a.wg.Add(1)
		go a.run(task)
	}
}

// run executes one task and reports the result. Execution uses a background
// context (not the agent's) so a shutdown signal lets in-flight tasks finish
// during drain rather than killing them mid-flight.
func (a *Agent) run(task *distschedv1.TaskAssignment) {
	defer a.wg.Done()
	defer a.finish(task.GetTaskId())

	a.log.Info("task started", "task_id", task.GetTaskId(), "job_id", task.GetJobId(), "attempt", task.GetAttempt())
	result, output, errMsg := execute(context.Background(), task.GetSpec())
	a.report(task.GetTaskId(), result, output, errMsg)

	a.log.Info("task finished",
		"task_id", task.GetTaskId(), "job_id", task.GetJobId(), "result", result.String(), "error", errMsg)
}

// report sends a result, retrying a few times so a transient scheduler error
// (which keeps the lease) does not silently drop the outcome.
func (a *Agent) report(taskID string, result distschedv1.TaskResult, output, errMsg string) {
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := a.client.ReportTask(ctx, &distschedv1.ReportTaskRequest{
			WorkerId: a.workerID,
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
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	a.log.Error("report permanently failed; task may be re-leased after lease expiry", "task_id", taskID)
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

// tryStart claims a slot for taskID, returning false if already running or at
// capacity.
func (a *Agent) tryStart(taskID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.running[taskID]; ok {
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
	a.mu.Unlock()
}
