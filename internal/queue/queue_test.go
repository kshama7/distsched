package queue

import (
	"sync"
	"testing"
	"time"
)

func TestPriorityOrdering(t *testing.T) {
	q := New()
	base := time.Unix(1000, 0)
	// Insert out of order; higher priority should pop first.
	q.Push("low", 1, base)
	q.Push("high", 10, base)
	q.Push("mid", 5, base)

	want := []string{"high", "mid", "low"}
	for _, w := range want {
		got, ok := q.Pop()
		if !ok || got != w {
			t.Fatalf("Pop() = %q, %v; want %q", got, ok, w)
		}
	}
	if _, ok := q.Pop(); ok {
		t.Fatalf("expected empty queue")
	}
}

func TestFIFOWithinPriority(t *testing.T) {
	q := New()
	base := time.Unix(2000, 0)
	// Same priority: older created_at pops first.
	q.Push("second", 5, base.Add(time.Second))
	q.Push("first", 5, base)
	q.Push("third", 5, base.Add(2*time.Second))

	for _, w := range []string{"first", "second", "third"} {
		got, _ := q.Pop()
		if got != w {
			t.Fatalf("Pop() = %q; want %q", got, w)
		}
	}
}

func TestDuplicatePushIgnored(t *testing.T) {
	q := New()
	if !q.Push("a", 1, time.Now()) {
		t.Fatal("first push should succeed")
	}
	if q.Push("a", 99, time.Now()) {
		t.Fatal("duplicate push should report false")
	}
	if q.Len() != 1 {
		t.Fatalf("Len() = %d; want 1", q.Len())
	}
}

func TestRemove(t *testing.T) {
	q := New()
	now := time.Now()
	q.Push("a", 1, now)
	q.Push("b", 2, now)
	q.Push("c", 3, now)

	if !q.Remove("b") {
		t.Fatal("Remove(b) should report true")
	}
	if q.Remove("missing") {
		t.Fatal("Remove(missing) should report false")
	}
	if q.Contains("b") {
		t.Fatal("b should be gone")
	}
	// Remaining order should still be valid: c (3) then a (1).
	for _, w := range []string{"c", "a"} {
		got, _ := q.Pop()
		if got != w {
			t.Fatalf("Pop() = %q; want %q", got, w)
		}
	}
}

// TestConcurrentAccess runs the queue under the race detector to confirm the
// locking is sound.
func TestConcurrentAccess(t *testing.T) {
	q := New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := string(rune('A' + n%26))
			q.Push(id+string(rune('0'+n/26)), int32(n), time.Now())
			q.Len()
			q.Pop()
		}(i)
	}
	wg.Wait()
}
