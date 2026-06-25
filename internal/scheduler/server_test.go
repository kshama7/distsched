package scheduler_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/scheduler"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startServer brings up a scheduler on a random port backed by dataDir and
// returns its address. The server is shut down on test cleanup.
func startServer(t *testing.T, dataDir string) (*scheduler.Server, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv, err := scheduler.New(scheduler.Config{
		NodeID:     "test",
		ListenAddr: lis.Addr().String(),
		DataDir:    dataDir,
	}, testLogger())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Shutdown)
	return srv, lis.Addr().String()
}

func dialJob(t *testing.T, addr string) distschedv1.JobServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return distschedv1.NewJobServiceClient(conn)
}

func dialWorker(t *testing.T, addr string) distschedv1.WorkerServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return distschedv1.NewWorkerServiceClient(conn)
}

func sampleJob(name string, priority int32) *distschedv1.Job {
	return &distschedv1.Job{
		Name:     name,
		Priority: priority,
		Spec:     &distschedv1.TaskSpec{Command: "echo", Args: []string{name}},
	}
}

func TestSubmitGetAndPersist(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, t.TempDir())
	client := dialJob(t, addr)

	resp, err := client.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: sampleJob("first", 5)})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if resp.GetJobId() == "" {
		t.Fatal("expected assigned job id")
	}

	got, err := client.GetJob(ctx, &distschedv1.GetJobRequest{JobId: resp.GetJobId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.GetJob().GetState() != distschedv1.JobState_JOB_STATE_QUEUED {
		t.Fatalf("state = %s; want QUEUED", got.GetJob().GetState())
	}
}

func TestSubmitValidation(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, t.TempDir())
	client := dialJob(t, addr)

	// Missing command must be rejected.
	_, err := client.SubmitJob(ctx, &distschedv1.SubmitJobRequest{
		Job: &distschedv1.Job{Name: "bad"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s; want InvalidArgument", status.Code(err))
	}
}

func TestListJobsFilter(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, t.TempDir())
	client := dialJob(t, addr)

	for i, p := range []int32{1, 5, 9} {
		if _, err := client.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: sampleJob(string(rune('a'+i)), p)}); err != nil {
			t.Fatal(err)
		}
	}

	all, err := client.ListJobs(ctx, &distschedv1.ListJobsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all.GetJobs()) != 3 {
		t.Fatalf("listed %d; want 3", len(all.GetJobs()))
	}

	queued, err := client.ListJobs(ctx, &distschedv1.ListJobsRequest{
		StateFilter: distschedv1.JobState_JOB_STATE_QUEUED,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(queued.GetJobs()) != 3 {
		t.Fatalf("queued %d; want 3", len(queued.GetJobs()))
	}
}

func TestCancelJob(t *testing.T) {
	ctx := context.Background()
	srv, addr := startServer(t, t.TempDir())
	client := dialJob(t, addr)

	resp, _ := client.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: sampleJob("cancelme", 5)})
	if srv.QueueDepth() != 1 {
		t.Fatalf("queue depth = %d; want 1", srv.QueueDepth())
	}

	cancel, err := client.CancelJob(ctx, &distschedv1.CancelJobRequest{JobId: resp.GetJobId()})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if cancel.GetJob().GetState() != distschedv1.JobState_JOB_STATE_CANCELED {
		t.Fatalf("state = %s; want CANCELED", cancel.GetJob().GetState())
	}
	if srv.QueueDepth() != 0 {
		t.Fatalf("queue depth = %d; want 0 after cancel", srv.QueueDepth())
	}

	// Canceling again is idempotent.
	if _, err := client.CancelJob(ctx, &distschedv1.CancelJobRequest{JobId: resp.GetJobId()}); err != nil {
		t.Fatalf("second cancel should be idempotent, got %v", err)
	}
}

func TestWorkerRegisterAndHeartbeat(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, t.TempDir())
	client := dialWorker(t, addr)

	reg, err := client.RegisterWorker(ctx, &distschedv1.RegisterWorkerRequest{
		Worker: &distschedv1.WorkerInfo{Address: "1.2.3.4:9000", Capacity: 8},
	})
	if err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if reg.GetWorkerId() == "" {
		t.Fatal("expected assigned worker id")
	}

	hb, err := client.Heartbeat(ctx, &distschedv1.HeartbeatRequest{WorkerId: reg.GetWorkerId()})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !hb.GetOk() {
		t.Fatal("heartbeat ok = false for known worker")
	}

	// Unknown worker is told to re-register.
	unknown, err := client.Heartbeat(ctx, &distschedv1.HeartbeatRequest{WorkerId: "ghost"})
	if err != nil {
		t.Fatalf("Heartbeat(unknown): %v", err)
	}
	if unknown.GetOk() {
		t.Fatal("heartbeat ok = true for unknown worker")
	}
}

// TestRecoveryAcrossRestart submits jobs, stops the scheduler, and starts a new
// one on the same data dir, asserting the queue is rebuilt from BoltDB.
func TestRecoveryAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	srv1, addr1 := startServer(t, dir)
	client1 := dialJob(t, addr1)
	for i := 0; i < 3; i++ {
		if _, err := client1.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: sampleJob("j", int32(i))}); err != nil {
			t.Fatal(err)
		}
	}
	if srv1.QueueDepth() != 3 {
		t.Fatalf("pre-restart depth = %d; want 3", srv1.QueueDepth())
	}
	// Stop the first server so it releases the BoltDB file lock.
	srv1.Shutdown()
	time.Sleep(50 * time.Millisecond)

	srv2, _ := startServer(t, dir)
	if got := srv2.QueueDepth(); got != 3 {
		t.Fatalf("recovered queue depth = %d; want 3", got)
	}
}
