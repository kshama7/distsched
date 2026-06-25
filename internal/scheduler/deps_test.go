package scheduler_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

// assertNoTask fails if a task is dispatched within the window.
func assertNoTask(t *testing.T, wc distschedv1.WorkerServiceClient, workerID string, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		resp, err := wc.PollTask(context.Background(), &distschedv1.PollTaskRequest{WorkerId: workerID, MaxTasks: 1})
		if err != nil {
			t.Fatalf("PollTask: %v", err)
		}
		if len(resp.GetTasks()) > 0 {
			t.Fatalf("unexpected task dispatched for job %s", resp.GetTasks()[0].GetJobId())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDAGDependencyGating(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, t.TempDir())
	jc := dialJob(t, addr)
	wc := dialWorker(t, addr)
	workerID := registerWorker(t, wc)

	a := sampleJob("A", 5)
	a.Id = "A"
	if _, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: a}); err != nil {
		t.Fatal(err)
	}
	b := sampleJob("B", 5)
	b.Id = "B"
	b.DependsOn = []string{"A"}
	if _, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: b}); err != nil {
		t.Fatal(err)
	}

	// B must start PENDING.
	gb, _ := jc.GetJob(ctx, &distschedv1.GetJobRequest{JobId: "B"})
	if gb.GetJob().GetState() != distschedv1.JobState_JOB_STATE_PENDING {
		t.Fatalf("B state = %s; want PENDING", gb.GetJob().GetState())
	}

	// Only A is dispatchable.
	first := pollUntilTask(t, wc, workerID, time.Second)
	if first.GetJobId() != "A" {
		t.Fatalf("first dispatched = %s; want A", first.GetJobId())
	}
	assertNoTask(t, wc, workerID, 150*time.Millisecond)

	// Completing A unblocks B.
	if _, err := wc.ReportTask(ctx, &distschedv1.ReportTaskRequest{
		WorkerId: workerID, TaskId: first.GetTaskId(),
		Result: distschedv1.TaskResult_TASK_RESULT_SUCCEEDED,
	}); err != nil {
		t.Fatal(err)
	}
	second := pollUntilTask(t, wc, workerID, time.Second)
	if second.GetJobId() != "B" {
		t.Fatalf("second dispatched = %s; want B", second.GetJobId())
	}
}

func TestDependencyCycleRejected(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, t.TempDir())
	jc := dialJob(t, addr)

	x := sampleJob("x", 1)
	x.Id = "X"
	x.DependsOn = []string{"Y"} // Y not yet submitted: legal, X is PENDING
	if _, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: x}); err != nil {
		t.Fatal(err)
	}
	y := sampleJob("y", 1)
	y.Id = "Y"
	y.DependsOn = []string{"X"} // closes the cycle X<->Y
	_, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: y})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cycle submit code = %s; want InvalidArgument", status.Code(err))
	}
}

func TestDependencyFailureCancelsDependent(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, t.TempDir())
	jc := dialJob(t, addr)
	wc := dialWorker(t, addr)
	workerID := registerWorker(t, wc)

	a := sampleJob("fail-a", 5)
	a.Id = "FA"
	a.RetryPolicy = &distschedv1.RetryPolicy{MaxAttempts: 1}
	if _, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: a}); err != nil {
		t.Fatal(err)
	}
	b := sampleJob("dependent-b", 5)
	b.Id = "FB"
	b.DependsOn = []string{"FA"}
	if _, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: b}); err != nil {
		t.Fatal(err)
	}

	task := pollUntilTask(t, wc, workerID, time.Second) // FA
	if _, err := wc.ReportTask(ctx, &distschedv1.ReportTaskRequest{
		WorkerId: workerID, TaskId: task.GetTaskId(),
		Result: distschedv1.TaskResult_TASK_RESULT_FAILED, Error: "boom",
	}); err != nil {
		t.Fatal(err)
	}

	// FA dead-letters (1 attempt), which cancels its dependent FB.
	waitForState(t, jc, "FA", distschedv1.JobState_JOB_STATE_FAILED, time.Second)
	waitForState(t, jc, "FB", distschedv1.JobState_JOB_STATE_CANCELED, time.Second)
}

func TestDelayedJob(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, t.TempDir())
	jc := dialJob(t, addr)
	wc := dialWorker(t, addr)
	workerID := registerWorker(t, wc)

	d := sampleJob("delayed", 5)
	d.Id = "D"
	d.Schedule = &distschedv1.Schedule{
		Kind: &distschedv1.Schedule_RunAt{RunAt: timestamppb.New(time.Now().Add(500 * time.Millisecond))},
	}
	if _, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: d}); err != nil {
		t.Fatal(err)
	}

	// Not dispatchable before its run_at time.
	assertNoTask(t, wc, workerID, 250*time.Millisecond)
	// Dispatchable after.
	task := pollUntilTask(t, wc, workerID, 2*time.Second)
	if task.GetJobId() != "D" {
		t.Fatalf("dispatched = %s; want D", task.GetJobId())
	}
}

func TestCronJobRecurs(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, t.TempDir())
	jc := dialJob(t, addr)
	wc := dialWorker(t, addr)
	workerID := registerWorker(t, wc)

	c := sampleJob("cron", 5)
	c.Id = "C"
	c.Schedule = &distschedv1.Schedule{Kind: &distschedv1.Schedule_CronExpr{CronExpr: "@every 400ms"}}
	if _, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: c}); err != nil {
		t.Fatal(err)
	}

	// First occurrence.
	t1 := pollUntilTask(t, wc, workerID, 2*time.Second)
	if t1.GetJobId() != "C" {
		t.Fatalf("occurrence 1 = %s; want C", t1.GetJobId())
	}
	if _, err := wc.ReportTask(ctx, &distschedv1.ReportTaskRequest{
		WorkerId: workerID, TaskId: t1.GetTaskId(),
		Result: distschedv1.TaskResult_TASK_RESULT_SUCCEEDED,
	}); err != nil {
		t.Fatal(err)
	}

	// A cron job re-arms rather than going terminal.
	got, _ := jc.GetJob(ctx, &distschedv1.GetJobRequest{JobId: "C"})
	if got.GetJob().GetState() == distschedv1.JobState_JOB_STATE_SUCCEEDED {
		t.Fatal("cron job should not be terminally SUCCEEDED")
	}

	// Second occurrence fires after the interval.
	t2 := pollUntilTask(t, wc, workerID, 2*time.Second)
	if t2.GetJobId() != "C" || t2.GetTaskId() == t1.GetTaskId() {
		t.Fatalf("occurrence 2 = %+v; want a new task for C", t2)
	}
}
