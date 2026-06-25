package ctl_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/ctl"
	"github.com/kshama7/distsched/internal/scheduler"
)

func startScheduler(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	srv, err := scheduler.New(scheduler.Config{NodeID: "t", ListenAddr: addr, DataDir: t.TempDir()},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Shutdown)
	return addr
}

func TestClientLifecycle(t *testing.T) {
	ctx := context.Background()
	addr := startScheduler(t)
	c := ctl.New([]string{addr})

	// status finds the single-node leader.
	st, err := c.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.GetLease().GetHolderId() != "t" {
		t.Fatalf("leader = %q; want t", st.GetLease().GetHolderId())
	}

	// submit -> get -> list -> cancel.
	id, err := c.Submit(ctx, ctl.SubmitParams{Name: "ctl-job", Command: "echo", Args: []string{"hi"}, Priority: 3})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if id == "" {
		t.Fatal("empty job id")
	}

	job, err := c.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if job.GetName() != "ctl-job" || job.GetState() != distschedv1.JobState_JOB_STATE_QUEUED {
		t.Fatalf("got %+v", job)
	}

	jobs, err := c.List(ctx, distschedv1.JobState_JOB_STATE_UNSPECIFIED)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("List = %d jobs, err=%v; want 1", len(jobs), err)
	}

	canceled, err := c.Cancel(ctx, id)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if canceled.GetState() != distschedv1.JobState_JOB_STATE_CANCELED {
		t.Fatalf("state = %s; want CANCELED", canceled.GetState())
	}

	// submit a delayed job and confirm the schedule round-trips.
	delayedID, err := c.Submit(ctx, ctl.SubmitParams{Command: "echo", After: time.Minute})
	if err != nil {
		t.Fatalf("Submit delayed: %v", err)
	}
	dj, _ := c.Get(ctx, delayedID)
	if dj.GetState() != distschedv1.JobState_JOB_STATE_QUEUED || dj.GetScheduledAt() == nil {
		t.Fatalf("delayed job not scheduled: %+v", dj)
	}
}
