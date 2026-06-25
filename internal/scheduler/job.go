package scheduler

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/ids"
	"github.com/kshama7/distsched/internal/store"
)

// SubmitJob validates and places a job. Depending on its dependencies and
// schedule the job starts QUEUED (ready now), PENDING (waiting on DAG
// predecessors or a future fire time), or CANCELED (a dependency already
// failed). Dependency cycles and malformed cron expressions are rejected.
func (s *Server) SubmitJob(_ context.Context, req *distschedv1.SubmitJobRequest) (*distschedv1.SubmitJobResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	job := req.GetJob()
	if job == nil {
		return nil, status.Error(codes.InvalidArgument, "job is required")
	}
	if job.GetSpec().GetCommand() == "" {
		return nil, status.Error(codes.InvalidArgument, "job.spec.command is required")
	}
	if expr := job.GetSchedule().GetCronExpr(); expr != "" {
		if _, err := nextCron(expr, time.Now()); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid cron_expr: %v", err)
		}
	}
	if job.GetId() == "" {
		job.Id = ids.New("job")
	}
	if len(job.GetDependsOn()) > 0 && s.wouldCreateCycle(job.GetId(), job.GetDependsOn()) {
		return nil, status.Error(codes.InvalidArgument, "dependencies form a cycle")
	}

	now := timestamppb.Now()
	job.Attempt = 0
	job.AssignedWorkerId = ""
	job.LastError = ""
	job.CreatedAt = now
	job.UpdatedAt = now

	s.placeNewJob(job, now.AsTime())

	s.log.Info("job submitted",
		"job_id", job.GetId(),
		"name", job.GetName(),
		"priority", job.GetPriority(),
		"state", job.GetState().String(),
		"depends_on", len(job.GetDependsOn()))
	return &distschedv1.SubmitJobResponse{JobId: job.GetId()}, nil
}

// GetJob returns a single job by ID.
func (s *Server) GetJob(_ context.Context, req *distschedv1.GetJobRequest) (*distschedv1.GetJobResponse, error) {
	job, err := s.loadJob(req.GetJobId())
	if err != nil {
		return nil, err
	}
	return &distschedv1.GetJobResponse{Job: job}, nil
}

// ListJobs returns jobs, optionally filtered by state. Pagination is simplified
// for Milestone 2: page_size caps the result count and no cursor is returned.
func (s *Server) ListJobs(_ context.Context, req *distschedv1.ListJobsRequest) (*distschedv1.ListJobsResponse, error) {
	all, err := s.store.ListJobs()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list jobs: %v", err)
	}
	filter := req.GetStateFilter()
	out := make([]*distschedv1.Job, 0, len(all))
	for _, j := range all {
		if filter != distschedv1.JobState_JOB_STATE_UNSPECIFIED && j.GetState() != filter {
			continue
		}
		out = append(out, j)
	}
	if ps := int(req.GetPageSize()); ps > 0 && len(out) > ps {
		out = out[:ps]
	}
	return &distschedv1.ListJobsResponse{Jobs: out}, nil
}

// CancelJob marks a non-terminal job CANCELED and removes it from the queue.
// Canceling an already-canceled job is idempotent; canceling a job that has
// already succeeded or failed is rejected.
func (s *Server) CancelJob(_ context.Context, req *distschedv1.CancelJobRequest) (*distschedv1.CancelJobResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	job, err := s.loadJob(req.GetJobId())
	if err != nil {
		return nil, err
	}
	switch job.GetState() {
	case distschedv1.JobState_JOB_STATE_CANCELED:
		return &distschedv1.CancelJobResponse{Job: job}, nil
	case distschedv1.JobState_JOB_STATE_SUCCEEDED, distschedv1.JobState_JOB_STATE_FAILED:
		return nil, status.Errorf(codes.FailedPrecondition,
			"job %q already in terminal state %s", job.GetId(), job.GetState())
	}

	job.State = distschedv1.JobState_JOB_STATE_CANCELED
	job.UpdatedAt = timestamppb.Now()
	if err := s.putJob(job); err != nil {
		return nil, status.Errorf(codes.Internal, "persist cancel: %v", err)
	}
	s.queue.Remove(job.GetId())
	s.log.Info("job canceled", "job_id", job.GetId(), "queue_depth", s.queue.Len())

	// Cancel anything that depended on this job — it can never run now.
	s.cancelCascade(job.GetId(), "upstream dependency "+job.GetId()+" was canceled")
	return &distschedv1.CancelJobResponse{Job: job}, nil
}

// loadJob fetches a job, mapping a missing key to a gRPC NotFound status.
func (s *Server) loadJob(id string) (*distschedv1.Job, error) {
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	job, err := s.store.GetJob(id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "job %q not found", id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load job: %v", err)
	}
	return job, nil
}
