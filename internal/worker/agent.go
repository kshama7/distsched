// Package worker implements the worker agent: it registers with a scheduler and
// heartbeats on a fixed interval. Task pulling and execution arrive in
// Milestone 3.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

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
}

func (c *Config) withDefaults() {
	if c.Capacity <= 0 {
		c.Capacity = 4
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
}

// Agent is a worker process.
type Agent struct {
	cfg      Config
	log      *slog.Logger
	client   distschedv1.WorkerServiceClient
	workerID string
}

// New constructs an Agent.
func New(cfg Config, log *slog.Logger) *Agent {
	cfg.withDefaults()
	return &Agent{cfg: cfg, log: log}
}

// Run dials the scheduler, registers, and heartbeats until ctx is canceled.
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

	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.log.Info("worker shutting down", "worker_id", a.workerID)
			return nil
		case <-ticker.C:
			a.heartbeat(ctx)
		}
	}
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
			a.log.Info("registered with scheduler",
				"worker_id", a.workerID,
				"leader", resp.GetLeaderAddress())
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

// heartbeat sends one heartbeat; if the scheduler no longer knows this worker it
// re-registers.
func (a *Agent) heartbeat(ctx context.Context) {
	resp, err := a.client.Heartbeat(ctx, &distschedv1.HeartbeatRequest{WorkerId: a.workerID})
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
