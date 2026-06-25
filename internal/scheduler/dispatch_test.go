package scheduler_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/worker"
)

// registerWorker registers a worker and returns its ID.
func registerWorker(t *testing.T, wc distschedv1.WorkerServiceClient) string {
	t.Helper()
	reg, err := wc.RegisterWorker(context.Background(), &distschedv1.RegisterWorkerRequest{
		Worker: &distschedv1.WorkerInfo{Capacity: 4},
	})
	if err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	return reg.GetWorkerId()
}

// pollUntilTask polls until a task is leased or the deadline passes.
func pollUntilTask(t *testing.T, wc distschedv1.WorkerServiceClient, workerID string, within time.Duration) *distschedv1.TaskAssignment {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		resp, err := wc.PollTask(context.Background(), &distschedv1.PollTaskRequest{WorkerId: workerID, MaxTasks: 1})
		if err != nil {
			t.Fatalf("PollTask: %v", err)
		}
		if len(resp.GetTasks()) > 0 {
			return resp.GetTasks()[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no task leased within %v", within)
	return nil
}

// waitForState polls GetJob until the job reaches want or the deadline passes.
func waitForState(t *testing.T, jc distschedv1.JobServiceClient, jobID string, want distschedv1.JobState, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last distschedv1.JobState
	for time.Now().Before(deadline) {
		resp, err := jc.GetJob(context.Background(), &distschedv1.GetJobRequest{JobId: jobID})
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		last = resp.GetJob().GetState()
		if last == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s state = %s; want %s within %v", jobID, last, want, within)
}

// TestDispatchAndComplete exercises the raw lease/report cycle at the gRPC layer.
func TestDispatchAndComplete(t *testing.T) {
	ctx := context.Background()
	srv, addr := startServer(t, t.TempDir())
	jc := dialJob(t, addr)
	wc := dialWorker(t, addr)
	workerID := registerWorker(t, wc)

	sub, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: sampleJob("work", 5)})
	if err != nil {
		t.Fatal(err)
	}

	task := pollUntilTask(t, wc, workerID, time.Second)
	if task.GetJobId() != sub.GetJobId() || task.GetAttempt() != 1 {
		t.Fatalf("unexpected assignment: %+v", task)
	}
	if srv.InFlight() != 1 {
		t.Fatalf("InFlight = %d; want 1", srv.InFlight())
	}

	// The job should now be RUNNING.
	got, _ := jc.GetJob(ctx, &distschedv1.GetJobRequest{JobId: sub.GetJobId()})
	if got.GetJob().GetState() != distschedv1.JobState_JOB_STATE_RUNNING {
		t.Fatalf("state = %s; want RUNNING", got.GetJob().GetState())
	}

	if _, err := wc.ReportTask(ctx, &distschedv1.ReportTaskRequest{
		WorkerId: workerID, TaskId: task.GetTaskId(),
		Result: distschedv1.TaskResult_TASK_RESULT_SUCCEEDED,
	}); err != nil {
		t.Fatalf("ReportTask: %v", err)
	}

	waitForState(t, jc, sub.GetJobId(), distschedv1.JobState_JOB_STATE_SUCCEEDED, time.Second)
	if srv.InFlight() != 0 {
		t.Fatalf("InFlight = %d; want 0 after report", srv.InFlight())
	}
}

// TestDuplicateReportIgnored confirms a second report for the same task is a
// harmless ack (at-least-once / idempotent semantics).
func TestDuplicateReportIgnored(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, t.TempDir())
	jc := dialJob(t, addr)
	wc := dialWorker(t, addr)
	workerID := registerWorker(t, wc)

	sub, _ := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: sampleJob("dup", 1)})
	task := pollUntilTask(t, wc, workerID, time.Second)

	report := &distschedv1.ReportTaskRequest{
		WorkerId: workerID, TaskId: task.GetTaskId(),
		Result: distschedv1.TaskResult_TASK_RESULT_SUCCEEDED,
	}
	if _, err := wc.ReportTask(ctx, report); err != nil {
		t.Fatal(err)
	}
	// Second, duplicate report must still ack without error.
	resp, err := wc.ReportTask(ctx, report)
	if err != nil || !resp.GetAck() {
		t.Fatalf("duplicate report: ack=%v err=%v", resp.GetAck(), err)
	}
	waitForState(t, jc, sub.GetJobId(), distschedv1.JobState_JOB_STATE_SUCCEEDED, time.Second)
}

// TestRetryThenDeadLetter drives a job through repeated failures into the
// dead-letter queue, asserting the attempt count and backoff requeue.
func TestRetryThenDeadLetter(t *testing.T) {
	ctx := context.Background()
	srv, addr := startServer(t, t.TempDir())
	jc := dialJob(t, addr)
	wc := dialWorker(t, addr)
	workerID := registerWorker(t, wc)

	job := sampleJob("flaky", 5)
	job.RetryPolicy = &distschedv1.RetryPolicy{
		MaxAttempts:       2,
		InitialBackoff:    durationpb.New(time.Millisecond),
		MaxBackoff:        durationpb.New(5 * time.Millisecond),
		BackoffMultiplier: 2,
	}
	sub, _ := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: job})

	// Attempt 1 fails -> backoff retry.
	t1 := pollUntilTask(t, wc, workerID, time.Second)
	if t1.GetAttempt() != 1 {
		t.Fatalf("first attempt = %d; want 1", t1.GetAttempt())
	}
	failTask(t, wc, workerID, t1.GetTaskId())

	// Attempt 2 fails -> exhausted -> dead-letter.
	t2 := pollUntilTask(t, wc, workerID, time.Second)
	if t2.GetAttempt() != 2 {
		t.Fatalf("second attempt = %d; want 2", t2.GetAttempt())
	}
	failTask(t, wc, workerID, t2.GetTaskId())

	waitForState(t, jc, sub.GetJobId(), distschedv1.JobState_JOB_STATE_FAILED, time.Second)
	if srv.DeadLetterCount() != 1 {
		t.Fatalf("dead-letter count = %d; want 1", srv.DeadLetterCount())
	}
}

func failTask(t *testing.T, wc distschedv1.WorkerServiceClient, workerID, taskID string) {
	t.Helper()
	if _, err := wc.ReportTask(context.Background(), &distschedv1.ReportTaskRequest{
		WorkerId: workerID, TaskId: taskID,
		Result: distschedv1.TaskResult_TASK_RESULT_FAILED, Error: "boom",
	}); err != nil {
		t.Fatalf("ReportTask(failed): %v", err)
	}
}

// TestEndToEndWithWorkerAgent runs the real worker.Agent against the scheduler,
// executing actual subprocesses: a succeeding job and a failing one that
// dead-letters.
func TestEndToEndWithWorkerAgent(t *testing.T) {
	ctx := context.Background()
	srv, addr := startServer(t, t.TempDir())
	jc := dialJob(t, addr)

	agentCtx, cancel := context.WithCancel(ctx)
	agent := worker.New(worker.Config{
		SchedulerAddr:     addr,
		Capacity:          2,
		HeartbeatInterval: 200 * time.Millisecond,
		PollInterval:      20 * time.Millisecond,
	}, testLogger())
	agentDone := make(chan struct{})
	go func() { _ = agent.Run(agentCtx); close(agentDone) }()
	t.Cleanup(func() { cancel(); <-agentDone })

	// Success: `true` exits 0.
	ok, _ := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: &distschedv1.Job{
		Name: "ok", Spec: &distschedv1.TaskSpec{Command: "true"},
	}})
	waitForState(t, jc, ok.GetJobId(), distschedv1.JobState_JOB_STATE_SUCCEEDED, 3*time.Second)

	// Failure: `false` exits 1; max_attempts=1 dead-letters immediately.
	bad, _ := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: &distschedv1.Job{
		Name:        "bad",
		Spec:        &distschedv1.TaskSpec{Command: "false"},
		RetryPolicy: &distschedv1.RetryPolicy{MaxAttempts: 1},
	}})
	waitForState(t, jc, bad.GetJobId(), distschedv1.JobState_JOB_STATE_FAILED, 3*time.Second)
	if srv.DeadLetterCount() != 1 {
		t.Fatalf("dead-letter count = %d; want 1", srv.DeadLetterCount())
	}
}
