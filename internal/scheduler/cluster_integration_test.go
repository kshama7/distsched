package scheduler_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/scheduler"
)

// startCluster brings up an n-node in-process cluster on loopback with fast
// election timing, returning the servers and their addresses.
func startCluster(t *testing.T, n int) ([]*scheduler.Server, []string) {
	t.Helper()

	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	members := make([]scheduler.Member, n)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = lis
		addrs[i] = lis.Addr().String()
		members[i] = scheduler.Member{ID: fmt.Sprintf("node-%d", i), Address: addrs[i]}
	}

	servers := make([]*scheduler.Server, n)
	for i := 0; i < n; i++ {
		srv, err := scheduler.New(scheduler.Config{
			NodeID:             members[i].ID,
			ListenAddr:         addrs[i],
			DataDir:            t.TempDir(),
			Members:            members,
			HeartbeatInterval:  40 * time.Millisecond,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
		}, testLogger())
		if err != nil {
			t.Fatalf("new server %d: %v", i, err)
		}
		servers[i] = srv
		go func(s *scheduler.Server, l net.Listener) { _ = s.Serve(l) }(srv, listeners[i])
	}
	t.Cleanup(func() {
		for _, s := range servers {
			if s != nil {
				s.Shutdown()
			}
		}
	})
	return servers, addrs
}

// waitLeader returns the index of the unique leader, or fails after within.
func waitLeader(t *testing.T, servers []*scheduler.Server, within time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		idx := -1
		count := 0
		for i, s := range servers {
			if s != nil && s.IsLeader() {
				count++
				idx = i
			}
		}
		if count == 1 {
			time.Sleep(60 * time.Millisecond) // confirm stability
			count = 0
			for _, s := range servers {
				if s != nil && s.IsLeader() {
					count++
				}
			}
			if count == 1 {
				return idx
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("no unique leader within %v", within)
	return -1
}

func TestClusterElectsOneLeader(t *testing.T) {
	servers, _ := startCluster(t, 3)
	leader := waitLeader(t, servers, 4*time.Second)
	t.Logf("leader is node-%d at term %d", leader, servers[leader].Term())
}

func TestFollowerRedirectsWrites(t *testing.T) {
	servers, addrs := startCluster(t, 3)
	leader := waitLeader(t, servers, 4*time.Second)

	// Pick a follower and submit to it: must be rejected with FailedPrecondition.
	follower := (leader + 1) % 3
	fc := dialJob(t, addrs[follower])
	_, err := fc.SubmitJob(context.Background(), &distschedv1.SubmitJobRequest{Job: sampleJob("nope", 1)})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("follower SubmitJob code = %s; want FailedPrecondition", status.Code(err))
	}
}

func TestReplicationToFollowers(t *testing.T) {
	ctx := context.Background()
	servers, addrs := startCluster(t, 3)
	leader := waitLeader(t, servers, 4*time.Second)

	lc := dialJob(t, addrs[leader])
	sub, err := lc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: sampleJob("replicated", 5)})
	if err != nil {
		t.Fatalf("SubmitJob to leader: %v", err)
	}

	// Every follower should receive the job via replication (GetJob reads local
	// store and is served by any node).
	for i, addr := range addrs {
		if i == leader {
			continue
		}
		fc := dialJob(t, addr)
		found := false
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			resp, err := fc.GetJob(ctx, &distschedv1.GetJobRequest{JobId: sub.GetJobId()})
			if err == nil && resp.GetJob().GetId() == sub.GetJobId() {
				found = true
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !found {
			t.Fatalf("job not replicated to node-%d within timeout", i)
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	ctx := context.Background()
	servers, addrs := startCluster(t, 3)
	leader := waitLeader(t, servers, 4*time.Second)
	oldTerm := servers[leader].Term()

	// Submit a job, confirm it replicates to a follower, then kill the leader.
	lc := dialJob(t, addrs[leader])
	sub, err := lc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: sampleJob("survivor", 7)})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	// Give replication a moment to propagate before the leader dies.
	time.Sleep(300 * time.Millisecond)

	servers[leader].Shutdown()
	servers[leader] = nil

	newLeader := waitLeader(t, servers, 4*time.Second)
	if newLeader == leader {
		t.Fatal("expected a different leader after failover")
	}
	if servers[newLeader].Term() <= oldTerm {
		t.Fatalf("new term %d not greater than old %d", servers[newLeader].Term(), oldTerm)
	}
	t.Logf("failover: node-%d (term %d) -> node-%d (term %d)", leader, oldTerm, newLeader, servers[newLeader].Term())

	// The pre-failover job must still be retrievable from the new leader, and the
	// new leader must accept new writes.
	nc := dialJob(t, addrs[newLeader])
	got, err := nc.GetJob(ctx, &distschedv1.GetJobRequest{JobId: sub.GetJobId()})
	if err != nil || got.GetJob().GetId() != sub.GetJobId() {
		t.Fatalf("survivor job missing on new leader: err=%v", err)
	}
	if _, err := nc.SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: sampleJob("post-failover", 3)}); err != nil {
		t.Fatalf("new leader rejected write: %v", err)
	}
}
