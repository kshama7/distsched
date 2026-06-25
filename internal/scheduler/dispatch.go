package scheduler

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/ids"
	"github.com/kshama7/distsched/internal/retry"
)

// maxBatch caps how many tasks a single PollTask may lease, regardless of the
// worker's requested count.
const maxBatch = 64

// PollTask leases up to the requested number of ready jobs to a worker. Each
// lease moves its job QUEUED -> RUNNING, persists that transition, and records
// an in-memory lease keyed by a freshly minted task_id (the idempotency key the
// worker echoes back via ReportTask).
func (s *Server) PollTask(_ context.Context, req *distschedv1.PollTaskRequest) (*distschedv1.PollTaskResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if !s.workerKnown(req.GetWorkerId()) {
		return nil, status.Errorf(codes.FailedPrecondition, "unknown worker %q; re-register", req.GetWorkerId())
	}

	n := int(req.GetMaxTasks())
	if n <= 0 {
		n = 1
	}
	if n > maxBatch {
		n = maxBatch
	}

	now := time.Now()
	out := make([]*distschedv1.TaskAssignment, 0, n)
	for len(out) < n {
		jobID, ok := s.queue.Pop()
		if !ok {
			break
		}
		job, err := s.store.GetJob(jobID)
		if err != nil {
			s.log.Warn("dispatch: load job", "job_id", jobID, "err", err)
			continue
		}
		// A stale queue entry (e.g. the job was canceled) is simply dropped.
		if job.GetState() != distschedv1.JobState_JOB_STATE_QUEUED {
			continue
		}

		attempt := job.GetAttempt() + 1
		taskID := ids.New("task")
		deadline := now.Add(s.cfg.LeaseTTL)

		job.State = distschedv1.JobState_JOB_STATE_RUNNING
		job.Attempt = attempt
		job.AssignedWorkerId = req.GetWorkerId()
		job.ScheduledAt = nil
		job.StartedAt = timestamppb.New(now)
		job.UpdatedAt = timestamppb.New(now)
		if err := s.putJob(job); err != nil {
			// Persisting the RUNNING transition failed; put the job back so it is
			// not lost and stop handing out this batch.
			s.log.Error("dispatch: persist job", "job_id", jobID, "err", err)
			s.queue.Push(jobID, job.GetPriority(), job.GetCreatedAt().AsTime())
			break
		}

		s.addLease(&lease{taskID: taskID, jobID: jobID, workerID: req.GetWorkerId(), attempt: attempt, deadline: deadline})
		out = append(out, &distschedv1.TaskAssignment{
			TaskId:        taskID,
			JobId:         jobID,
			Attempt:       attempt,
			Spec:          job.GetSpec(),
			LeaseDeadline: timestamppb.New(deadline),
		})
		s.log.Info("task dispatched",
			"task_id", taskID, "job_id", jobID, "attempt", attempt, "worker_id", req.GetWorkerId())
	}
	return &distschedv1.PollTaskResponse{Tasks: out}, nil
}

// ReportTask records a terminal result for a leased task. The lease is removed
// only after the resulting state change is durably persisted, so a transient
// store error lets the worker safely re-report. Reports for unknown task IDs
// (duplicates, or a task already reaped) are acked and ignored — execution is
// at-least-once and idempotent.
func (s *Server) ReportTask(_ context.Context, req *distschedv1.ReportTaskRequest) (*distschedv1.ReportTaskResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	ls, ok := s.getLease(req.GetTaskId())
	if !ok {
		s.log.Debug("report for unknown/duplicate task; ignoring", "task_id", req.GetTaskId())
		return &distschedv1.ReportTaskResponse{Ack: true}, nil
	}

	job, err := s.store.GetJob(ls.jobID)
	if err != nil {
		s.removeLease(req.GetTaskId())
		return &distschedv1.ReportTaskResponse{Ack: true}, nil
	}
	// Ignore a report for a job that is no longer running (e.g. canceled).
	if job.GetState() != distschedv1.JobState_JOB_STATE_RUNNING {
		s.removeLease(req.GetTaskId())
		return &distschedv1.ReportTaskResponse{Ack: true}, nil
	}

	now := time.Now()
	switch req.GetResult() {
	case distschedv1.TaskResult_TASK_RESULT_SUCCEEDED:
		job.State = distschedv1.JobState_JOB_STATE_SUCCEEDED
		job.AssignedWorkerId = ""
		job.LastError = ""
		job.FinishedAt = timestamppb.New(now)
		job.UpdatedAt = timestamppb.New(now)
		if err := s.putJob(job); err != nil {
			return nil, status.Errorf(codes.Internal, "persist success: %v", err)
		}
		s.removeLease(req.GetTaskId())
		s.log.Info("job succeeded", "job_id", job.GetId(), "attempt", job.GetAttempt())

	case distschedv1.TaskResult_TASK_RESULT_FAILED:
		if err := s.handleFailure(job, req.GetError(), now); err != nil {
			return nil, err
		}
		s.removeLease(req.GetTaskId())

	default:
		// Unspecified result: leave the lease in place for re-reporting.
		return nil, status.Error(codes.InvalidArgument, "task result is required")
	}

	return &distschedv1.ReportTaskResponse{Ack: true}, nil
}

// handleFailure either schedules a backoff retry or dead-letters the job once
// retries are exhausted.
func (s *Server) handleFailure(job *distschedv1.Job, errMsg string, now time.Time) error {
	job.LastError = errMsg
	job.AssignedWorkerId = ""
	job.UpdatedAt = timestamppb.New(now)

	maxAttempts := retry.MaxAttempts(job.GetRetryPolicy())
	if int(job.GetAttempt()) < maxAttempts {
		delay := retry.Backoff(job.GetRetryPolicy(), int(job.GetAttempt()))
		job.State = distschedv1.JobState_JOB_STATE_QUEUED
		job.ScheduledAt = timestamppb.New(now.Add(delay))
		if err := s.putJob(job); err != nil {
			return status.Errorf(codes.Internal, "persist retry: %v", err)
		}
		s.armEnqueue(job.GetId(), delay)
		s.log.Info("job retry scheduled",
			"job_id", job.GetId(), "attempt", job.GetAttempt(), "max_attempts", maxAttempts,
			"delay", delay.String(), "error", errMsg)
		return nil
	}

	job.State = distschedv1.JobState_JOB_STATE_FAILED
	job.FinishedAt = timestamppb.New(now)
	// Write the dead-letter index entry before committing the terminal state, so
	// an observer that sees the job FAILED is guaranteed to also see it in the
	// dead-letter queue.
	if err := s.store.PutDeadLetter(job); err != nil {
		return status.Errorf(codes.Internal, "persist dead-letter: %v", err)
	}
	if err := s.putJob(job); err != nil {
		return status.Errorf(codes.Internal, "persist failure: %v", err)
	}
	s.log.Warn("job dead-lettered",
		"job_id", job.GetId(), "attempts", job.GetAttempt(), "error", errMsg)
	return nil
}

func (s *Server) addLease(l *lease) {
	s.dispatchMu.Lock()
	s.leases[l.taskID] = l
	s.dispatchMu.Unlock()
}

func (s *Server) getLease(taskID string) (*lease, bool) {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	l, ok := s.leases[taskID]
	return l, ok
}

func (s *Server) removeLease(taskID string) {
	s.dispatchMu.Lock()
	delete(s.leases, taskID)
	s.dispatchMu.Unlock()
}
