package store

import (
	"errors"
	"path/filepath"
	"testing"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestJobRoundTrip(t *testing.T) {
	st := openTemp(t)
	job := &distschedv1.Job{
		Id:       "job-1",
		Name:     "hello",
		Priority: 7,
		State:    distschedv1.JobState_JOB_STATE_QUEUED,
		Spec:     &distschedv1.TaskSpec{Command: "echo", Args: []string{"hi"}},
	}
	if err := st.PutJob(job); err != nil {
		t.Fatalf("PutJob: %v", err)
	}
	got, err := st.GetJob("job-1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.GetName() != "hello" || got.GetPriority() != 7 || got.GetSpec().GetCommand() != "echo" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestGetMissingJob(t *testing.T) {
	st := openTemp(t)
	_, err := st.GetJob("nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v; want ErrNotFound", err)
	}
}

func TestListAndDeleteJob(t *testing.T) {
	st := openTemp(t)
	for _, id := range []string{"a", "b", "c"} {
		if err := st.PutJob(&distschedv1.Job{Id: id}); err != nil {
			t.Fatal(err)
		}
	}
	jobs, err := st.ListJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 3 {
		t.Fatalf("ListJobs len = %d; want 3", len(jobs))
	}
	if err := st.DeleteJob("b"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetJob("b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("b should be deleted, got err=%v", err)
	}
}

func TestWorkerRoundTrip(t *testing.T) {
	st := openTemp(t)
	w := &distschedv1.WorkerInfo{Id: "w-1", Capacity: 4, State: distschedv1.WorkerState_WORKER_STATE_ALIVE}
	if err := st.PutWorker(w); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetWorker("w-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetCapacity() != 4 {
		t.Fatalf("capacity = %d; want 4", got.GetCapacity())
	}
	workers, err := st.ListWorkers()
	if err != nil || len(workers) != 1 {
		t.Fatalf("ListWorkers = %v, len=%d", err, len(workers))
	}
}

// TestPersistenceAcrossReopen confirms data survives closing and reopening the
// same file — the durability guarantee the checkpoint-recovery path relies on.
func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	st1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st1.PutJob(&distschedv1.Job{Id: "durable", Name: "survivor"}); err != nil {
		t.Fatal(err)
	}
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	got, err := st2.GetJob("durable")
	if err != nil {
		t.Fatalf("GetJob after reopen: %v", err)
	}
	if got.GetName() != "survivor" {
		t.Fatalf("name = %q; want survivor", got.GetName())
	}
}
