package cluster

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// memStore is an in-memory StateStore.
type memStore struct {
	mu       sync.Mutex
	term     int64
	votedFor string
}

func (m *memStore) LoadState() (int64, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.term, m.votedFor, nil
}

func (m *memStore) SaveState(term int64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term, m.votedFor = term, votedFor
	return nil
}

// harness wires several Nodes together with an in-memory transport and a
// simulated heartbeat loop. Nodes can be marked down to simulate crashes.
type harness struct {
	mu    sync.Mutex
	nodes map[string]*Node
	down  map[string]bool
}

func (h *harness) isDown(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.down[id]
}

func (h *harness) setDown(id string, d bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.down[id] = d
}

// memTransport routes votes through the harness.
type memTransport struct {
	h    *harness
	self string
}

func (t *memTransport) SendVote(_ context.Context, peerID string, term int64, candidateID string) (bool, int64, error) {
	if t.h.isDown(t.self) || t.h.isDown(peerID) {
		return false, 0, io.ErrClosedPipe
	}
	t.h.mu.Lock()
	peer := t.h.nodes[peerID]
	t.h.mu.Unlock()
	g, ct := peer.HandleVote(term, candidateID)
	return g, ct, nil
}

func newHarness(ctx context.Context, ids []string) *harness {
	h := &harness{nodes: make(map[string]*Node), down: make(map[string]bool)}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, id := range ids {
		n := New(Config{
			NodeID:             id,
			Members:            ids,
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 250 * time.Millisecond,
		}, &memTransport{h: h, self: id}, &memStore{}, log,
			func(int64) {}, func(int64) {})
		h.nodes[id] = n
	}
	for _, id := range ids {
		h.nodes[id].Start(ctx)
	}
	go h.heartbeatLoop(ctx)
	return h
}

// heartbeatLoop makes every up leader assert leadership to up peers, mirroring
// the scheduler's Replicate heartbeat.
func (h *harness) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.mu.Lock()
			ids := make([]string, 0, len(h.nodes))
			for id := range h.nodes {
				ids = append(ids, id)
			}
			h.mu.Unlock()
			for _, lid := range ids {
				if h.isDown(lid) || !h.nodes[lid].IsLeader() {
					continue
				}
				term := h.nodes[lid].Term()
				for _, fid := range ids {
					if fid == lid || h.isDown(fid) {
						continue
					}
					h.nodes[fid].ObserveLeader(term, lid)
				}
			}
		}
	}
}

// leaders returns the IDs of up nodes that believe they are leader.
func (h *harness) upLeaders() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for id, n := range h.nodes {
		if !h.down[id] && n.IsLeader() {
			out = append(out, id)
		}
	}
	return out
}

// waitForSingleLeader polls until exactly one up node is leader, returning its ID.
func waitForSingleLeader(t *testing.T, h *harness, within time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if l := h.upLeaders(); len(l) == 1 {
			// Confirm it stays stable briefly.
			time.Sleep(80 * time.Millisecond)
			if l2 := h.upLeaders(); len(l2) == 1 && l2[0] == l[0] {
				return l[0]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no single stable leader within %v (leaders=%v)", within, h.upLeaders())
	return ""
}

func TestElectsSingleLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHarness(ctx, []string{"a", "b", "c"})

	leader := waitForSingleLeader(t, h, 3*time.Second)
	t.Logf("elected leader: %s", leader)

	// The other two must be followers and agree on the term.
	lterm := h.nodes[leader].Term()
	for _, id := range []string{"a", "b", "c"} {
		if id == leader {
			continue
		}
		if h.nodes[id].IsLeader() {
			t.Fatalf("two leaders: %s and %s", leader, id)
		}
		if h.nodes[id].Term() != lterm {
			t.Fatalf("follower %s term=%d; leader term=%d", id, h.nodes[id].Term(), lterm)
		}
	}
}

func TestFailoverElectsNewLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHarness(ctx, []string{"a", "b", "c"})

	old := waitForSingleLeader(t, h, 3*time.Second)
	oldTerm := h.nodes[old].Term()

	// Crash the leader.
	h.setDown(old, true)

	newLeader := waitForSingleLeader(t, h, 3*time.Second)
	if newLeader == old {
		t.Fatalf("new leader %s == old leader", newLeader)
	}
	if h.nodes[newLeader].Term() <= oldTerm {
		t.Fatalf("new term %d not greater than old term %d", h.nodes[newLeader].Term(), oldTerm)
	}
	t.Logf("failover: %s (term %d) -> %s (term %d)", old, oldTerm, newLeader, h.nodes[newLeader].Term())
}

func TestNoMajorityNoLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHarness(ctx, []string{"a", "b", "c"})

	leader := waitForSingleLeader(t, h, 3*time.Second)

	// Kill the leader and one follower, leaving a single survivor that cannot
	// reach a majority (needs 2 of 3 votes). A partitioned pre-existing leader
	// does not self-demote, so the survivor must be a follower for this test.
	var survivor, otherFollower string
	for _, id := range []string{"a", "b", "c"} {
		if id == leader {
			continue
		}
		if survivor == "" {
			survivor = id
		} else {
			otherFollower = id
		}
	}
	h.setDown(leader, true)
	h.setDown(otherFollower, true)

	time.Sleep(1500 * time.Millisecond)
	if l := h.upLeaders(); len(l) != 0 {
		t.Fatalf("expected no leader without majority, got %v", l)
	}
	if h.nodes[survivor].IsLeader() {
		t.Fatalf("survivor %s should not be leader without majority", survivor)
	}
}

func TestSingleNodeImmediateLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leaderFired := make(chan int64, 1)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	n := New(Config{NodeID: "solo", Members: []string{"solo"}},
		&memTransport{h: &harness{nodes: map[string]*Node{}, down: map[string]bool{}}, self: "solo"},
		&memStore{}, log,
		func(term int64) { leaderFired <- term }, func(int64) {})
	n.Start(ctx)

	if !n.IsLeader() {
		t.Fatal("single node should be leader immediately after Start")
	}
	select {
	case <-leaderFired:
	case <-time.After(time.Second):
		t.Fatal("onLeader not invoked for single node")
	}
}
