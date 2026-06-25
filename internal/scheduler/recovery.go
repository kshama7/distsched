package scheduler

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

// reaperLoop runs on the leader and periodically reclaims work from dead workers
// and expired leases.
func (s *Server) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.node.IsLeader() {
				s.reapOnce()
			}
		}
	}
}

// reapOnce performs one liveness + lease sweep:
//  1. Workers past the heartbeat timeout are marked DEAD (or SUSPECT at half the
//     timeout); a dead worker's in-flight tasks are requeued immediately.
//  2. Any lease past its deadline is requeued, covering a worker that vanished
//     or stalled without being declared dead yet.
func (s *Server) reapOnce() {
	now := time.Now()
	timeout := s.cfg.HeartbeatTimeout

	// Phase 1: worker liveness. Decide state transitions under lock using cloned
	// values, then publish and requeue outside the lock.
	s.workersMu.Lock()
	var toPersist []*distschedv1.WorkerInfo
	var deadIDs []string
	for id, cur := range s.workers {
		last := cur.GetLastHeartbeat().AsTime()
		age := now.Sub(last)
		target := distschedv1.WorkerState_WORKER_STATE_ALIVE
		switch {
		case age > timeout:
			target = distschedv1.WorkerState_WORKER_STATE_DEAD
		case age > timeout/2:
			target = distschedv1.WorkerState_WORKER_STATE_SUSPECT
		}
		if target != distschedv1.WorkerState_WORKER_STATE_ALIVE && cur.GetState() != target {
			w := proto.Clone(cur).(*distschedv1.WorkerInfo)
			w.State = target
			toPersist = append(toPersist, w)
			if target == distschedv1.WorkerState_WORKER_STATE_DEAD {
				deadIDs = append(deadIDs, id)
			}
		}
	}
	s.workersMu.Unlock()

	for _, w := range toPersist {
		if err := s.putWorker(w); err != nil {
			s.log.Error("persist worker liveness", "worker_id", w.GetId(), "err", err)
		}
		s.log.Warn("worker liveness change", "worker_id", w.GetId(), "state", w.GetState().String())
	}
	for _, id := range deadIDs {
		s.requeueLeasesForWorker(id, "worker missed heartbeats")
	}

	// Phase 2: expired leases.
	s.dispatchMu.Lock()
	var expired []string
	for taskID, l := range s.leases {
		if now.After(l.deadline) {
			expired = append(expired, taskID)
		}
	}
	s.dispatchMu.Unlock()
	for _, taskID := range expired {
		s.requeueLease(taskID, "lease deadline exceeded")
	}
}

// requeueLeasesForWorker requeues every in-flight task leased to a worker.
func (s *Server) requeueLeasesForWorker(workerID, reason string) {
	s.dispatchMu.Lock()
	var taskIDs []string
	for taskID, l := range s.leases {
		if l.workerID == workerID {
			taskIDs = append(taskIDs, taskID)
		}
	}
	s.dispatchMu.Unlock()
	for _, taskID := range taskIDs {
		s.requeueLease(taskID, reason)
	}
}

// requeueLease drops a lease and treats its lost attempt as a failure, so the
// job is retried with backoff (or dead-lettered if it has exhausted its
// retries). A late report for this task_id afterwards finds no lease and is
// harmlessly ignored — the at-least-once, idempotent contract.
func (s *Server) requeueLease(taskID, reason string) {
	l, ok := s.getLease(taskID)
	if !ok {
		return // already reclaimed or completed
	}
	s.removeLease(taskID)

	job, err := s.store.GetJob(l.jobID)
	if err != nil {
		s.log.Warn("requeue: load job", "job_id", l.jobID, "err", err)
		return
	}
	if job.GetState() != distschedv1.JobState_JOB_STATE_RUNNING {
		return // job already completed/canceled
	}
	s.log.Warn("requeuing lost task",
		"task_id", taskID, "job_id", l.jobID, "worker_id", l.workerID, "reason", reason)
	if err := s.handleFailure(job, reason, time.Now()); err != nil {
		s.log.Error("requeue: handle failure", "job_id", l.jobID, "err", err)
	}
}
