// Package queue implements the scheduler's in-memory ready queue: a
// thread-safe binary heap ordered by (priority desc, created_at asc, id asc).
// It holds only jobs that are eligible to run now; it is a cache rebuilt from
// the durable store on startup.
package queue

import (
	"container/heap"
	"sync"
	"time"
)

// item is one queued job. index is maintained by container/heap.
type item struct {
	jobID     string
	priority  int32
	createdAt time.Time
	index     int
}

// innerHeap is the container/heap implementation. Higher priority sorts first;
// ties break by older created_at, then by job ID for a total, deterministic
// order.
type innerHeap []*item

func (h innerHeap) Len() int { return len(h) }

func (h innerHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.priority != b.priority {
		return a.priority > b.priority
	}
	if !a.createdAt.Equal(b.createdAt) {
		return a.createdAt.Before(b.createdAt)
	}
	return a.jobID < b.jobID
}

func (h innerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *innerHeap) Push(x any) {
	it := x.(*item)
	it.index = len(*h)
	*h = append(*h, it)
}

func (h *innerHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	it.index = -1
	*h = old[:n-1]
	return it
}

// PriorityQueue is a concurrency-safe priority queue keyed by job ID. A job ID
// present in the queue is unique; pushing a duplicate ID is a no-op.
type PriorityQueue struct {
	mu   sync.Mutex
	h    innerHeap
	byID map[string]*item
}

// New returns an empty PriorityQueue.
func New() *PriorityQueue {
	return &PriorityQueue{byID: make(map[string]*item)}
}

// Push enqueues a job. If the job ID is already queued it is left unchanged and
// Push reports false.
func (q *PriorityQueue) Push(jobID string, priority int32, createdAt time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.byID[jobID]; ok {
		return false
	}
	it := &item{jobID: jobID, priority: priority, createdAt: createdAt}
	heap.Push(&q.h, it)
	q.byID[jobID] = it
	return true
}

// Pop removes and returns the highest-priority job ID. ok is false if empty.
func (q *PriorityQueue) Pop() (jobID string, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.h.Len() == 0 {
		return "", false
	}
	it := heap.Pop(&q.h).(*item)
	delete(q.byID, it.jobID)
	return it.jobID, true
}

// Remove deletes a specific job from the queue (e.g. on cancel). Reports
// whether it was present.
func (q *PriorityQueue) Remove(jobID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.byID[jobID]
	if !ok {
		return false
	}
	heap.Remove(&q.h, it.index)
	delete(q.byID, jobID)
	return true
}

// Contains reports whether a job ID is currently queued.
func (q *PriorityQueue) Contains(jobID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.byID[jobID]
	return ok
}

// Len returns the number of queued jobs.
func (q *PriorityQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.h.Len()
}

// Clear removes all queued jobs. Used when a node steps down from leadership and
// must stop dispatching.
func (q *PriorityQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.h = nil
	q.byID = make(map[string]*item)
}
