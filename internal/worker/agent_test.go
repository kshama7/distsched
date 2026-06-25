package worker

import (
	"io"
	"log/slog"
	"testing"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestTryStartIdempotencyAndCapacity verifies the worker suppresses duplicate
// task deliveries (running and recently-done) and respects capacity.
func TestTryStartIdempotencyAndCapacity(t *testing.T) {
	a := New(Config{Capacity: 1}, testLogger())

	if !a.tryStart("t1") {
		t.Fatal("first start should succeed")
	}
	if a.tryStart("t1") {
		t.Fatal("duplicate while running should be suppressed")
	}
	if a.tryStart("t2") {
		t.Fatal("should be at capacity (1 running)")
	}

	a.finish("t1")

	if a.tryStart("t1") {
		t.Fatal("re-delivery of a completed task should be suppressed (idempotent)")
	}
	if !a.tryStart("t3") {
		t.Fatal("a fresh task should start once capacity frees up")
	}
}
