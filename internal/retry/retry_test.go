package retry

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

func TestMaxAttempts(t *testing.T) {
	if got := MaxAttempts(nil); got != DefaultMaxAttempts {
		t.Fatalf("nil policy = %d; want %d", got, DefaultMaxAttempts)
	}
	if got := MaxAttempts(&distschedv1.RetryPolicy{MaxAttempts: 0}); got != DefaultMaxAttempts {
		t.Fatalf("zero policy = %d; want default", got)
	}
	if got := MaxAttempts(&distschedv1.RetryPolicy{MaxAttempts: 5}); got != 5 {
		t.Fatalf("explicit = %d; want 5", got)
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	p := &distschedv1.RetryPolicy{
		MaxAttempts:       10,
		InitialBackoff:    durationpb.New(100 * time.Millisecond),
		MaxBackoff:        durationpb.New(time.Second),
		BackoffMultiplier: 2,
		Jitter:            0.2,
	}
	// Expected base (pre-jitter): 100ms, 200ms, 400ms, 800ms, then capped 1s.
	bases := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second,
		time.Second,
	}
	for i, base := range bases {
		attempt := i + 1
		lo := time.Duration(float64(base) * 0.8)
		hi := time.Duration(float64(base) * 1.2)
		// Sample a few times since jitter is randomized.
		for s := 0; s < 20; s++ {
			got := Backoff(p, attempt)
			if got < lo || got > hi {
				t.Fatalf("attempt %d: Backoff=%v; want in [%v,%v]", attempt, got, lo, hi)
			}
		}
	}
}

func TestBackoffDefaultsWhenNil(t *testing.T) {
	got := Backoff(nil, 1)
	// Default initial is 1s with ±20% jitter.
	if got < 800*time.Millisecond || got > 1200*time.Millisecond {
		t.Fatalf("default Backoff(1) = %v; want ~1s ±20%%", got)
	}
}
