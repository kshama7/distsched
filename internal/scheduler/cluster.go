package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

// voteTimeout and replicateTimeout bound peer RPCs so a dead peer never stalls
// an election round or a heartbeat tick.
const (
	voteTimeout      = 300 * time.Millisecond
	replicateTimeout = 500 * time.Millisecond
)

// notLeaderError tells a client to retry against the current leader.
func notLeaderError(leaderAddr string) error {
	return status.Errorf(codes.FailedPrecondition, "not leader; leader_addr=%s", leaderAddr)
}

// peerTransport dials peer schedulers' SchedulerService and implements
// cluster.Transport (SendVote). It also carries the leader's Replicate sender.
type peerTransport struct {
	self    string
	members map[string]string // id -> address
	log     *slog.Logger

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newPeerTransport(members map[string]string, self string, log *slog.Logger) *peerTransport {
	return &peerTransport{self: self, members: members, log: log, conns: make(map[string]*grpc.ClientConn)}
}

func (p *peerTransport) client(peerID string) (distschedv1.SchedulerServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[peerID]; ok {
		return distschedv1.NewSchedulerServiceClient(c), nil
	}
	conn, err := grpc.NewClient(p.members[peerID], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.conns[peerID] = conn
	return distschedv1.NewSchedulerServiceClient(conn), nil
}

func (p *peerTransport) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = make(map[string]*grpc.ClientConn)
}

// SendVote implements cluster.Transport.
func (p *peerTransport) SendVote(ctx context.Context, peerID string, term int64, candidateID string) (bool, int64, error) {
	c, err := p.client(peerID)
	if err != nil {
		return false, 0, err
	}
	cctx, cancel := context.WithTimeout(ctx, voteTimeout)
	defer cancel()
	resp, err := c.RequestVote(cctx, &distschedv1.RequestVoteRequest{CandidateId: candidateID, Term: term})
	if err != nil {
		return false, 0, err
	}
	return resp.GetVoteGranted(), resp.GetTerm(), nil
}

// sendReplicate ships a heartbeat + entries to a follower.
func (p *peerTransport) sendReplicate(ctx context.Context, peerID string, term int64, leaderID string, entries []*distschedv1.ReplicationEntry) (ok bool, currentTerm, appliedThrough int64, err error) {
	c, cerr := p.client(peerID)
	if cerr != nil {
		return false, 0, 0, cerr
	}
	cctx, cancel := context.WithTimeout(ctx, replicateTimeout)
	defer cancel()
	resp, rerr := c.Replicate(cctx, &distschedv1.ReplicateRequest{LeaderId: leaderID, Term: term, Entries: entries})
	if rerr != nil {
		return false, 0, 0, rerr
	}
	return resp.GetOk(), resp.GetCurrentTerm(), resp.GetAppliedThrough(), nil
}

// --- SchedulerService handlers ---

// RequestVote handles a candidate's vote request via the election state machine.
func (s *Server) RequestVote(_ context.Context, req *distschedv1.RequestVoteRequest) (*distschedv1.RequestVoteResponse, error) {
	granted, term := s.node.HandleVote(req.GetTerm(), req.GetCandidateId())
	return &distschedv1.RequestVoteResponse{VoteGranted: granted, Term: term}, nil
}

// Replicate is the leader's heartbeat + metadata shipment. A follower accepts it
// if the term is current (resetting its election timer) and applies the entries,
// or rejects a stale leader by returning ok=false with its higher term.
func (s *Server) Replicate(_ context.Context, req *distschedv1.ReplicateRequest) (*distschedv1.ReplicateResponse, error) {
	accepted, currentTerm := s.node.ObserveLeader(req.GetTerm(), req.GetLeaderId())
	if !accepted {
		return &distschedv1.ReplicateResponse{Ok: false, CurrentTerm: currentTerm}, nil
	}
	applied := s.applyEntries(req.GetEntries())
	return &distschedv1.ReplicateResponse{Ok: true, CurrentTerm: currentTerm, AppliedThrough: applied}, nil
}

// applyEntries applies replicated mutations to the local store (follower side)
// and returns the highest seq applied. Writes go straight to the store, not
// through putJob/putWorker, so a follower does not re-replicate.
func (s *Server) applyEntries(entries []*distschedv1.ReplicationEntry) int64 {
	var last int64
	for _, e := range entries {
		switch m := e.GetMutation().(type) {
		case *distschedv1.ReplicationEntry_UpsertJob:
			if err := s.store.PutJob(m.UpsertJob); err != nil {
				s.log.Error("replicate: put job", "err", err)
			}
		case *distschedv1.ReplicationEntry_UpsertWorker:
			if err := s.store.PutWorker(m.UpsertWorker); err != nil {
				s.log.Error("replicate: put worker", "err", err)
			} else {
				s.workersMu.Lock()
				s.workers[m.UpsertWorker.GetId()] = m.UpsertWorker
				s.workersMu.Unlock()
			}
		case *distschedv1.ReplicationEntry_DeleteJobId:
			if err := s.store.DeleteJob(m.DeleteJobId); err != nil {
				s.log.Error("replicate: delete job", "err", err)
			}
		}
		if e.GetSeq() > last {
			last = e.GetSeq()
		}
	}
	return last
}

// GetClusterStatus reports the current lease, membership, and known workers. It
// is served by any node.
func (s *Server) GetClusterStatus(_ context.Context, _ *distschedv1.GetClusterStatusRequest) (*distschedv1.GetClusterStatusResponse, error) {
	leaderID := s.node.LeaderID()
	term := s.node.Term()
	nodes := make([]*distschedv1.SchedulerNode, 0, len(s.members))
	for id, addr := range s.members {
		role := distschedv1.SchedulerRole_SCHEDULER_ROLE_FOLLOWER
		if id == leaderID {
			role = distschedv1.SchedulerRole_SCHEDULER_ROLE_LEADER
		}
		nodes = append(nodes, &distschedv1.SchedulerNode{Id: id, Address: addr, Role: role})
	}
	workers, err := s.store.ListWorkers()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list workers: %v", err)
	}
	return &distschedv1.GetClusterStatusResponse{
		Lease:      &distschedv1.LeaseInfo{HolderId: leaderID, Term: term},
		Schedulers: nodes,
		Workers:    workers,
	}, nil
}

// replicationLoop is the leader's heartbeat/replication cadence.
func (s *Server) replicationLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.node.IsLeader() {
				s.broadcastReplicate(ctx)
			}
		}
	}
}

// broadcastReplicate sends each follower the entries it has not yet acked
// (heartbeat-only if caught up), and steps down if a follower reports a higher
// term.
func (s *Server) broadcastReplicate(ctx context.Context) {
	term := s.node.Term()
	for id := range s.members {
		if id == s.cfg.NodeID {
			continue
		}
		entries := s.entriesForPeer(id)
		ok, peerTerm, applied, err := s.xport.sendReplicate(ctx, id, term, s.cfg.NodeID, entries)
		if err != nil {
			continue // follower unreachable; retry next tick
		}
		if !ok {
			s.node.MaybeStepDown(peerTerm)
			continue
		}
		s.ackPeer(id, applied)
	}
}

// entriesForPeer returns replication entries with seq greater than the peer's
// acked cursor.
func (s *Server) entriesForPeer(peerID string) []*distschedv1.ReplicationEntry {
	s.replMu.Lock()
	defer s.replMu.Unlock()
	acked := s.peerAcked[peerID]
	var out []*distschedv1.ReplicationEntry
	for _, e := range s.replLog {
		if e.GetSeq() > acked {
			out = append(out, e)
		}
	}
	return out
}

func (s *Server) ackPeer(peerID string, applied int64) {
	if applied <= 0 {
		return
	}
	s.replMu.Lock()
	if applied > s.peerAcked[peerID] {
		s.peerAcked[peerID] = applied
	}
	s.replMu.Unlock()
}
