// Package cluster implements lease-based leader election for a set of scheduler
// replicas. It uses the well-understood, testable half of Raft — terms,
// randomized election timeouts, and majority voting — but deliberately omits
// Raft's replicated-log commit machinery. Leadership is a renewable lease: the
// leader asserts it via periodic heartbeats (the scheduler's Replicate RPC,
// reported here through ObserveLeader); if a follower misses heartbeats for an
// election timeout it campaigns for a new term.
//
// Safety: at most one leader per term, because a candidate needs votes from a
// majority of members and each member grants at most one vote per term. A
// monotonically increasing term fences stale leaders.
package cluster

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// Role is a node's current role.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Transport sends RequestVote RPCs to peers. Heartbeats (Replicate) are sent by
// the scheduler layer and reported back into the Node via ObserveLeader.
type Transport interface {
	// SendVote requests a vote from peerID for term. It returns whether the vote
	// was granted and the peer's current term.
	SendVote(ctx context.Context, peerID string, term int64, candidateID string) (granted bool, peerTerm int64, err error)
}

// StateStore persists the two values leader election must not lose across a
// restart: the current term and the vote cast in it.
type StateStore interface {
	LoadState() (term int64, votedFor string, err error)
	SaveState(term int64, votedFor string) error
}

// Config configures a Node.
type Config struct {
	// NodeID is this node's ID.
	NodeID string
	// Members is the full set of member IDs including this node.
	Members []string
	// ElectionTimeoutMin/Max bound the randomized election timeout.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

func (c *Config) withDefaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 800 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 1600 * time.Millisecond
	}
	if len(c.Members) == 0 {
		c.Members = []string{c.NodeID}
	}
}

// Node is the per-replica election state machine.
type Node struct {
	cfg       Config
	transport Transport
	store     StateStore
	log       *slog.Logger

	onLeader   func(term int64)
	onStepDown func(term int64)

	mu               sync.Mutex
	role             Role
	currentTerm      int64
	votedFor         string
	leaderID         string
	electionDeadline time.Time
}

// New constructs a Node. onLeader is invoked when this node becomes leader for a
// term; onStepDown when it ceases to be leader. Both may run concurrently with
// other work and should be quick or hand off to a goroutine.
func New(cfg Config, transport Transport, store StateStore, log *slog.Logger, onLeader, onStepDown func(term int64)) *Node {
	cfg.withDefaults()
	return &Node{
		cfg:        cfg,
		transport:  transport,
		store:      store,
		log:        log.With("component", "cluster", "node_id", cfg.NodeID),
		onLeader:   onLeader,
		onStepDown: onStepDown,
		role:       Follower,
	}
}

func (n *Node) majority() int { return len(n.cfg.Members)/2 + 1 }

func (n *Node) peers() []string {
	out := make([]string, 0, len(n.cfg.Members)-1)
	for _, m := range n.cfg.Members {
		if m != n.cfg.NodeID {
			out = append(out, m)
		}
	}
	return out
}

// Start loads persisted state and begins the election loop. For a single-member
// cluster it becomes leader synchronously (and invokes onLeader) before
// returning, so a standalone scheduler is immediately writable.
func (n *Node) Start(ctx context.Context) {
	if term, votedFor, err := n.store.LoadState(); err != nil {
		n.log.Warn("load persisted election state", "err", err)
	} else {
		n.mu.Lock()
		n.currentTerm, n.votedFor = term, votedFor
		n.mu.Unlock()
	}

	if n.majority() <= 1 {
		n.mu.Lock()
		n.currentTerm++
		n.votedFor = n.cfg.NodeID
		n.persistLocked()
		n.role = Leader
		n.leaderID = n.cfg.NodeID
		term := n.currentTerm
		n.mu.Unlock()
		n.log.Info("single-node cluster: assuming leadership", "term", term)
		n.onLeader(term)
		go n.run(ctx) // harmless no-op loop; keeps lifecycle uniform
		return
	}

	n.mu.Lock()
	n.resetElectionTimerLocked()
	n.mu.Unlock()
	go n.run(ctx)
}

func (n *Node) run(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			campaign := n.role != Leader && time.Now().After(n.electionDeadline)
			n.mu.Unlock()
			if campaign {
				n.startElection(ctx)
			}
		}
	}
}

type voteResult struct {
	granted  bool
	peerTerm int64
	ok       bool // false on transport error
}

// startElection transitions to Candidate for a new term and solicits votes. It
// becomes leader on a majority, or steps down if it learns of a higher term.
func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.cfg.NodeID
	n.persistLocked()
	n.resetElectionTimerLocked()
	term := n.currentTerm
	peers := n.peers()
	n.mu.Unlock()

	n.log.Info("starting election", "term", term)

	ch := make(chan voteResult, len(peers))
	for _, p := range peers {
		go func(peerID string) {
			granted, peerTerm, err := n.transport.SendVote(ctx, peerID, term, n.cfg.NodeID)
			ch <- voteResult{granted: granted, peerTerm: peerTerm, ok: err == nil}
		}(p)
	}

	votes := 1 // vote for self
	needed := n.majority()
	for i := 0; i < len(peers); i++ {
		r := <-ch
		if !r.ok {
			continue
		}
		n.mu.Lock()
		if n.role != Candidate || n.currentTerm != term {
			n.mu.Unlock() // superseded
			return
		}
		if r.peerTerm > n.currentTerm {
			n.becomeFollowerLocked(r.peerTerm)
			n.mu.Unlock()
			return
		}
		if r.granted {
			votes++
			if votes >= needed {
				n.becomeLeaderLocked(term)
				n.mu.Unlock()
				return
			}
		}
		n.mu.Unlock()
	}
}

// becomeLeaderLocked promotes this node to leader for term and fires onLeader.
func (n *Node) becomeLeaderLocked(term int64) {
	n.role = Leader
	n.leaderID = n.cfg.NodeID
	n.log.Info("won election; assuming leadership", "term", term, "votes_needed", n.majority(), "members", len(n.cfg.Members))
	go n.onLeader(term)
}

// HandleVote processes a RequestVote from a candidate and returns whether the
// vote is granted along with this node's current term.
func (n *Node) HandleVote(term int64, candidateID string) (granted bool, currentTerm int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term < n.currentTerm {
		return false, n.currentTerm
	}
	if term > n.currentTerm {
		n.becomeFollowerLocked(term)
	}
	if n.votedFor == "" || n.votedFor == candidateID {
		n.votedFor = candidateID
		n.persistLocked()
		n.resetElectionTimerLocked()
		return true, n.currentTerm
	}
	return false, n.currentTerm
}

// ObserveLeader is called when this node receives a heartbeat (Replicate) from a
// putative leader. It returns false if the caller's term is stale (the caller
// must step down); otherwise this node accepts the leader and resets its
// election timer.
func (n *Node) ObserveLeader(term int64, leaderID string) (accepted bool, currentTerm int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term < n.currentTerm {
		return false, n.currentTerm
	}
	if term > n.currentTerm {
		n.becomeFollowerLocked(term)
	} else if n.role == Candidate {
		n.role = Follower
	} else if n.role == Leader {
		// Same-term heartbeat from another leader is impossible under majority
		// voting; ignore defensively without disrupting our own leadership.
		return true, n.currentTerm
	}
	n.leaderID = leaderID
	n.resetElectionTimerLocked()
	return true, n.currentTerm
}

// MaybeStepDown steps down to follower if peerTerm is newer than the current
// term. The leader calls this with the term in a Replicate response so a stale
// leader that has been superseded relinquishes leadership.
func (n *Node) MaybeStepDown(peerTerm int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if peerTerm > n.currentTerm {
		n.becomeFollowerLocked(peerTerm)
	}
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// Role returns the current role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Term returns the current term.
func (n *Node) Term() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// LeaderID returns the ID of the leader this node currently recognizes (its own
// ID if it is the leader, or "" if unknown).
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Leader {
		return n.cfg.NodeID
	}
	return n.leaderID
}

func (n *Node) persistLocked() {
	if err := n.store.SaveState(n.currentTerm, n.votedFor); err != nil {
		n.log.Error("persist election state", "err", err)
	}
}

func (n *Node) resetElectionTimerLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	timeout := n.cfg.ElectionTimeoutMin
	if span > 0 {
		timeout += time.Duration(rand.Int63n(int64(span)))
	}
	n.electionDeadline = time.Now().Add(timeout)
}

// becomeFollowerLocked steps down to follower at the given term.
func (n *Node) becomeFollowerLocked(term int64) {
	wasLeader := n.role == Leader
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persistLocked()
	}
	n.role = Follower
	n.resetElectionTimerLocked()
	if wasLeader {
		t := n.currentTerm
		n.log.Info("stepping down from leader", "term", t)
		go n.onStepDown(t)
	}
}
