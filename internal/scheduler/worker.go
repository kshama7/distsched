package scheduler

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/ids"
)

// RegisterWorker records a worker and returns its assigned ID. A worker may
// supply its own ID to re-register after a restart; otherwise one is assigned.
func (s *Server) RegisterWorker(_ context.Context, req *distschedv1.RegisterWorkerRequest) (*distschedv1.RegisterWorkerResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	w := req.GetWorker()
	if w == nil {
		return nil, status.Error(codes.InvalidArgument, "worker is required")
	}
	if w.GetId() == "" {
		w.Id = ids.New("worker")
	}
	if w.GetCapacity() <= 0 {
		w.Capacity = 1
	}
	w.State = distschedv1.WorkerState_WORKER_STATE_ALIVE
	w.LastHeartbeat = timestamppb.Now()
	w.RunningTasks = 0

	if err := s.putWorker(w); err != nil {
		return nil, status.Errorf(codes.Internal, "persist worker: %v", err)
	}
	s.log.Info("worker registered",
		"worker_id", w.GetId(),
		"address", w.GetAddress(),
		"capacity", w.GetCapacity())
	return &distschedv1.RegisterWorkerResponse{
		WorkerId:      w.GetId(),
		LeaderAddress: s.selfAddr,
	}, nil
}

// Heartbeat refreshes a worker's liveness. An unknown worker is told to
// re-register via ok=false so a worker that outlived a scheduler restart with
// an empty store recovers cleanly.
func (s *Server) Heartbeat(_ context.Context, req *distschedv1.HeartbeatRequest) (*distschedv1.HeartbeatResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	s.workersMu.Lock()
	cur, known := s.workers[req.GetWorkerId()]
	s.workersMu.Unlock()
	if !known {
		return &distschedv1.HeartbeatResponse{Ok: false, LeaderAddress: s.selfAddr}, nil
	}

	// Clone before mutating: the worker cache pointer is read concurrently by the
	// liveness reaper, so we publish a fresh value rather than mutate in place.
	w := proto.Clone(cur).(*distschedv1.WorkerInfo)
	w.State = distschedv1.WorkerState_WORKER_STATE_ALIVE
	w.LastHeartbeat = timestamppb.Now()
	w.RunningTasks = int32(len(req.GetRunningTaskIds()))
	if err := s.putWorker(w); err != nil {
		return nil, status.Errorf(codes.Internal, "persist heartbeat: %v", err)
	}
	s.log.Debug("heartbeat", "worker_id", w.GetId(), "running_tasks", w.GetRunningTasks())
	return &distschedv1.HeartbeatResponse{Ok: true}, nil
}

// workerKnown reports whether a worker ID is currently registered.
func (s *Server) workerKnown(id string) bool {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	_, ok := s.workers[id]
	return ok
}
