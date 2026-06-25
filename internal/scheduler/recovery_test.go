package scheduler_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/scheduler"
)

// startServerWithConfig starts a single-node scheduler with a custom config,
// filling in the listen address and data dir.
func startServerWithConfig(t *testing.T, cfg scheduler.Config) (*scheduler.Server, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cfg.ListenAddr = lis.Addr().String()
	if cfg.DataDir == "" {
		cfg.DataDir = t.TempDir()
	}
	srv, err := scheduler.New(cfg, testLogger())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Shutdown)
	return srv, lis.Addr().String()
}

// fastRetryJob returns a job with near-zero backoff so requeues re-enter the
// queue almost immediately.
func fastRetryJob(name string) *distschedv1.Job {
	j := sampleJob(name, 5)
	j.RetryPolicy = &distschedv1.RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: durationpb.New(time.Millisecond),
		MaxBackoff:     durationpb.New(5 * time.Millisecond),
	}
	return j
}

// TestDeadWorkerRequeue confirms a worker that stops heartbeating is declared
// dead and its in-flight task is requeued for another attempt.
func TestDeadWorkerRequeue(t *testing.T) {
	ctx := context.Background()
	srv, addr := startServerWithConfig(t, scheduler.Config{
		NodeID:           "t",
		HeartbeatTimeout: 200 * time.Millisecond,
		ReaperInterval:   40 * time.Millisecond,
		LeaseTTL:         10 * time.Second, // large, so the dead-worker path (not lease expiry) fires
	})
	jc := dialJob(t, addr)
	wc := dialWorker(t, addr)
	workerID := registerWorker(t, wc)

	if _, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: fastRetryJob("flaky")}); err != nil {
		t.Fatal(err)
	}
	first := pollUntilTask(t, wc, workerID, time.Second)
	if first.GetAttempt() != 1 || srv.InFlight() != 1 {
		t.Fatalf("attempt=%d inflight=%d; want 1,1", first.GetAttempt(), srv.InFlight())
	}

	// Never heartbeat: the reaper should declare the worker dead and requeue the
	// task, which becomes available again as attempt 2.
	second := pollUntilTask(t, wc, workerID, 3*time.Second)
	if second.GetAttempt() != 2 {
		t.Fatalf("requeued attempt = %d; want 2", second.GetAttempt())
	}
}

// TestExpiredLeaseRequeue confirms a lease whose deadline passes is requeued even
// while the worker is still considered alive.
func TestExpiredLeaseRequeue(t *testing.T) {
	ctx := context.Background()
	srv, addr := startServerWithConfig(t, scheduler.Config{
		NodeID:           "t",
		HeartbeatTimeout: 30 * time.Second, // large, so the worker is never declared dead
		ReaperInterval:   30 * time.Millisecond,
		LeaseTTL:         120 * time.Millisecond,
	})
	jc := dialJob(t, addr)
	wc := dialWorker(t, addr)
	workerID := registerWorker(t, wc)

	if _, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: fastRetryJob("slow")}); err != nil {
		t.Fatal(err)
	}
	first := pollUntilTask(t, wc, workerID, time.Second)
	if first.GetAttempt() != 1 {
		t.Fatalf("attempt = %d; want 1", first.GetAttempt())
	}
	_ = srv

	// Do not report: the lease deadline lapses and the reaper requeues it.
	second := pollUntilTask(t, wc, workerID, 3*time.Second)
	if second.GetAttempt() != 2 {
		t.Fatalf("requeued attempt = %d; want 2", second.GetAttempt())
	}
}

// TestOrphanedRunningRecoveredOnRestart confirms a job left RUNNING by a crash is
// reclaimed and re-queued when the scheduler restarts.
func TestOrphanedRunningRecoveredOnRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	srv1, addr1 := startServerWithConfig(t, scheduler.Config{
		NodeID: "t", DataDir: dir, ReaperInterval: time.Hour, // disable reaping during this phase
	})
	jc1 := dialJob(t, addr1)
	wc1 := dialWorker(t, addr1)
	workerID := registerWorker(t, wc1)

	sub, err := jc1.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: fastRetryJob("orphan")})
	if err != nil {
		t.Fatal(err)
	}
	task := pollUntilTask(t, wc1, workerID, time.Second)
	if task.GetAttempt() != 1 {
		t.Fatalf("attempt = %d; want 1", task.GetAttempt())
	}
	// Job is now RUNNING with an in-memory-only lease. Crash the scheduler.
	srv1.Shutdown()
	time.Sleep(50 * time.Millisecond)

	// Restart on the same data dir: the RUNNING job must be reclaimed to QUEUED.
	srv2, addr2 := startServerWithConfig(t, scheduler.Config{NodeID: "t", DataDir: dir})
	jc2 := dialJob(t, addr2)
	waitForState(t, jc2, sub.GetJobId(), distschedv1.JobState_JOB_STATE_QUEUED, 2*time.Second)

	// And it should be dispatchable again.
	wc2 := dialWorker(t, addr2)
	workerID2 := registerWorker(t, wc2)
	again := pollUntilTask(t, wc2, workerID2, 2*time.Second)
	if again.GetJobId() != sub.GetJobId() {
		t.Fatalf("redispatched job = %s; want %s", again.GetJobId(), sub.GetJobId())
	}
	_ = srv2
}
