package scheduler_test

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/scheduler"
)

// metricValue scrapes the body for a single (unlabeled) metric line's value.
func metricValue(t *testing.T, body, name string) float64 {
	t.Helper()
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `\s+(\S+)$`)
	m := re.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("metric %q not found in /metrics output", name)
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		t.Fatalf("parse %q value %q: %v", name, m[1], err)
	}
	return v
}

func TestMetricsEndpoint(t *testing.T) {
	ctx := context.Background()
	srv, addr := startServerWithConfig(t, scheduler.Config{
		NodeID:      "t",
		MetricsAddr: "127.0.0.1:0",
	})
	jc := dialJob(t, addr)
	wc := dialWorker(t, addr)
	workerID := registerWorker(t, wc)

	if _, err := jc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: sampleJob("metric", 5)}); err != nil {
		t.Fatal(err)
	}
	task := pollUntilTask(t, wc, workerID, time.Second)
	if _, err := wc.ReportTask(ctx, &distschedv1.ReportTaskRequest{
		WorkerId: workerID, TaskId: task.GetTaskId(),
		Result: distschedv1.TaskResult_TASK_RESULT_SUCCEEDED,
	}); err != nil {
		t.Fatal(err)
	}

	maddr := srv.MetricsBoundAddr()
	if maddr == "" {
		t.Fatal("metrics endpoint not bound")
	}
	resp, err := http.Get("http://" + maddr + "/metrics")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	body := string(raw)

	if got := metricValue(t, body, "distsched_jobs_submitted_total"); got < 1 {
		t.Fatalf("jobs_submitted_total = %v; want >= 1", got)
	}
	if got := metricValue(t, body, "distsched_jobs_succeeded_total"); got < 1 {
		t.Fatalf("jobs_succeeded_total = %v; want >= 1", got)
	}
	if got := metricValue(t, body, "distsched_tasks_dispatched_total"); got < 1 {
		t.Fatalf("tasks_dispatched_total = %v; want >= 1", got)
	}
	// Live gauge: this single-node server is the leader.
	if got := metricValue(t, body, "distsched_is_leader"); got != 1 {
		t.Fatalf("is_leader = %v; want 1", got)
	}
}
