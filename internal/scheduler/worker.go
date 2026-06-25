package scheduler

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/ids"
)

// RegisterWorker records a worker and returns its assigned ID. A worker may
// supply its own ID to re-register after a restart; otherwise one is assigned.
func (s *Server) RegisterWorker(_ context.Context, req *distschedv1.RegisterWorkerRequest) (*distschedv1.RegisterWorkerResponse, error) {
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

	if err := s.persistWorker(w); err != nil {
		return nil, status.Errorf(codes.Internal, "persist worker: %v", err)
	}
	s.log.Info("worker registered",
		"worker_id", w.GetId(),
		"address", w.GetAddress(),
		"capacity", w.GetCapacity())
	return &distschedv1.RegisterWorkerResponse{
		WorkerId:      w.GetId(),
		LeaderAddress: s.cfg.ListenAddr,
	}, nil
}

// Heartbeat refreshes a worker's liveness. An unknown worker is told to
// re-register via ok=false so a worker that outlived a scheduler restart with
// an empty store recovers cleanly.
func (s *Server) Heartbeat(_ context.Context, req *distschedv1.HeartbeatRequest) (*distschedv1.HeartbeatResponse, error) {
	s.mu.Lock()
	w, known := s.workers[req.GetWorkerId()]
	s.mu.Unlock()
	if !known {
		return &distschedv1.HeartbeatResponse{Ok: false, LeaderAddress: s.cfg.ListenAddr}, nil
	}

	w.State = distschedv1.WorkerState_WORKER_STATE_ALIVE
	w.LastHeartbeat = timestamppb.Now()
	w.RunningTasks = int32(len(req.GetRunningTaskIds()))
	if err := s.persistWorker(w); err != nil {
		return nil, status.Errorf(codes.Internal, "persist heartbeat: %v", err)
	}
	s.log.Debug("heartbeat", "worker_id", w.GetId(), "running_tasks", w.GetRunningTasks())
	return &distschedv1.HeartbeatResponse{Ok: true}, nil
}

// PollTask returns no work in Milestone 2; task dispatch arrives in Milestone 3.
func (s *Server) PollTask(_ context.Context, _ *distschedv1.PollTaskRequest) (*distschedv1.PollTaskResponse, error) {
	return &distschedv1.PollTaskResponse{}, nil
}

// ReportTask acknowledges a result. Result handling (retry/dead-letter) arrives
// in Milestone 3.
func (s *Server) ReportTask(_ context.Context, _ *distschedv1.ReportTaskRequest) (*distschedv1.ReportTaskResponse, error) {
	return &distschedv1.ReportTaskResponse{Ack: true}, nil
}

// persistWorker writes the worker to the store and updates the in-memory cache
// under lock.
func (s *Server) persistWorker(w *distschedv1.WorkerInfo) error {
	if err := s.store.PutWorker(w); err != nil {
		return err
	}
	s.mu.Lock()
	s.workers[w.GetId()] = w
	s.mu.Unlock()
	return nil
}
