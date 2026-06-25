package scheduler

import (
	"time"

	cron "github.com/robfig/cron/v3"
	"google.golang.org/protobuf/types/known/timestamppb"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

// terminal reports whether a job state is final.
func terminal(s distschedv1.JobState) bool {
	switch s {
	case distschedv1.JobState_JOB_STATE_SUCCEEDED,
		distschedv1.JobState_JOB_STATE_FAILED,
		distschedv1.JobState_JOB_STATE_CANCELED:
		return true
	default:
		return false
	}
}

// nextCron parses a standard cron expression (also supporting @every / @daily
// descriptors) and returns the next fire time after `now`.
func nextCron(expr string, now time.Time) (time.Time, error) {
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(now), nil
}

// addDeps records reverse edges for a job's dependencies.
func (s *Server) addDeps(job *distschedv1.Job) {
	if len(job.GetDependsOn()) == 0 {
		return
	}
	s.depMu.Lock()
	for _, dep := range job.GetDependsOn() {
		set := s.revIndex[dep]
		if set == nil {
			set = make(map[string]struct{})
			s.revIndex[dep] = set
		}
		set[job.GetId()] = struct{}{}
	}
	s.depMu.Unlock()
}

// removeFromIndex drops jobID wherever it appears as a dependent.
func (s *Server) removeFromIndex(jobID string) {
	s.depMu.Lock()
	for dep, set := range s.revIndex {
		delete(set, jobID)
		if len(set) == 0 {
			delete(s.revIndex, dep)
		}
	}
	s.depMu.Unlock()
}

// dependentsOf returns a snapshot of the jobs waiting on depID.
func (s *Server) dependentsOf(depID string) []string {
	s.depMu.Lock()
	defer s.depMu.Unlock()
	set := s.revIndex[depID]
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

// depsSatisfied reports whether every dependency has SUCCEEDED. If any
// dependency is in a terminal non-success state, its ID is returned as failedDep
// (the dependent can never run). A missing or still-running dependency yields
// ready=false, failedDep="".
func (s *Server) depsSatisfied(job *distschedv1.Job) (ready bool, failedDep string) {
	for _, depID := range job.GetDependsOn() {
		dep, err := s.store.GetJob(depID)
		if err != nil {
			return false, "" // not submitted yet
		}
		switch dep.GetState() {
		case distschedv1.JobState_JOB_STATE_SUCCEEDED:
			continue
		case distschedv1.JobState_JOB_STATE_FAILED, distschedv1.JobState_JOB_STATE_CANCELED:
			return false, depID
		default:
			return false, "" // still pending/queued/running
		}
	}
	return true, ""
}

// wouldCreateCycle reports whether giving newID the given dependencies would
// introduce a cycle. The existing job graph is acyclic by this same check, so a
// new cycle can only pass through newID; a DFS over depends_on edges from newID
// that revisits a node on the recursion stack signals one.
func (s *Server) wouldCreateCycle(newID string, deps []string) bool {
	jobs, err := s.store.ListJobs()
	if err != nil {
		return false
	}
	graph := make(map[string][]string, len(jobs)+1)
	for _, j := range jobs {
		graph[j.GetId()] = j.GetDependsOn()
	}
	graph[newID] = deps

	visited := make(map[string]bool)
	stack := make(map[string]bool)
	var dfs func(string) bool
	dfs = func(n string) bool {
		visited[n] = true
		stack[n] = true
		for _, d := range graph[n] {
			if stack[d] {
				return true
			}
			if !visited[d] && dfs(d) {
				return true
			}
		}
		stack[n] = false
		return false
	}
	return dfs(newID)
}

// makeReady moves a job into QUEUED, honoring a delayed (run_at) or recurring
// (cron) schedule: it computes the fire time, persists, and arms the queue.
func (s *Server) makeReady(job *distschedv1.Job, now time.Time) {
	var delay time.Duration
	if sched := job.GetSchedule(); sched != nil {
		if expr := sched.GetCronExpr(); expr != "" {
			if next, err := nextCron(expr, now); err == nil {
				job.ScheduledAt = timestamppb.New(next)
				delay = time.Until(next)
			}
		} else if ra := sched.GetRunAt(); ra != nil && ra.AsTime().After(now) {
			job.ScheduledAt = ra
			delay = time.Until(ra.AsTime())
		}
	}
	job.State = distschedv1.JobState_JOB_STATE_QUEUED
	job.UpdatedAt = timestamppb.New(now)
	if err := s.putJob(job); err != nil {
		s.log.Error("makeReady: persist", "job_id", job.GetId(), "err", err)
		return
	}
	s.armEnqueue(job.GetId(), delay)
}

// placeNewJob assigns a freshly submitted job its initial state: CANCELED if a
// dependency already failed, PENDING if any dependency is unmet, otherwise ready
// (QUEUED, honoring any schedule).
func (s *Server) placeNewJob(job *distschedv1.Job, now time.Time) {
	ready, failedDep := s.depsSatisfied(job)
	switch {
	case failedDep != "":
		job.State = distschedv1.JobState_JOB_STATE_CANCELED
		job.LastError = "upstream dependency " + failedDep + " did not succeed"
		job.UpdatedAt = timestamppb.New(now)
		if err := s.putJob(job); err != nil {
			s.log.Error("place: persist canceled", "job_id", job.GetId(), "err", err)
		}
		s.metrics.JobsCanceled.Inc()
	case !ready:
		job.State = distschedv1.JobState_JOB_STATE_PENDING
		job.UpdatedAt = timestamppb.New(now)
		if err := s.putJob(job); err != nil {
			s.log.Error("place: persist pending", "job_id", job.GetId(), "err", err)
		}
		s.addDeps(job)
	default:
		s.makeReady(job, now)
	}
}

// tryPromote advances a PENDING job when its dependencies are satisfied, or
// cancels it (and its dependents) if a dependency failed.
func (s *Server) tryPromote(jobID string) {
	job, err := s.store.GetJob(jobID)
	if err != nil || job.GetState() != distschedv1.JobState_JOB_STATE_PENDING {
		return
	}
	ready, failedDep := s.depsSatisfied(job)
	if failedDep != "" {
		s.cancelCascade(jobID, "upstream dependency "+failedDep+" did not succeed")
		return
	}
	if ready {
		s.log.Info("dependencies satisfied; promoting job", "job_id", jobID)
		s.makeReady(job, time.Now())
		s.removeFromIndex(jobID)
	}
}

// promoteDependents re-evaluates everything waiting on a now-succeeded job.
func (s *Server) promoteDependents(depID string) {
	for _, dependent := range s.dependentsOf(depID) {
		s.tryPromote(dependent)
	}
}

// cancelCascade cancels a job and, transitively, every non-terminal job that
// depends on it, since they can never run.
func (s *Server) cancelCascade(jobID, reason string) {
	now := timestamppb.Now()
	visited := map[string]bool{}
	work := []string{jobID}
	for len(work) > 0 {
		cur := work[len(work)-1]
		work = work[:len(work)-1]
		if visited[cur] {
			continue
		}
		visited[cur] = true

		job, err := s.store.GetJob(cur)
		if err == nil && !terminal(job.GetState()) {
			job.State = distschedv1.JobState_JOB_STATE_CANCELED
			job.LastError = reason
			job.UpdatedAt = now
			if err := s.putJob(job); err != nil {
				s.log.Error("cancelCascade: persist", "job_id", cur, "err", err)
			}
			s.queue.Remove(cur)
			s.metrics.JobsCanceled.Inc()
			s.log.Warn("job canceled by dependency", "job_id", cur, "reason", reason)
		}
		dependents := s.dependentsOf(cur)
		s.removeFromIndex(cur)
		reason = "upstream dependency " + cur + " did not succeed"
		work = append(work, dependents...)
	}
}

// maybeRescheduleCron, if the job is a cron job, re-arms it for its next
// occurrence (resetting the attempt count) instead of letting it go terminal.
func (s *Server) maybeRescheduleCron(job *distschedv1.Job, now time.Time) bool {
	expr := job.GetSchedule().GetCronExpr()
	if expr == "" {
		return false
	}
	next, err := nextCron(expr, now)
	if err != nil {
		s.log.Error("cron parse on reschedule", "job_id", job.GetId(), "err", err)
		return false
	}
	job.State = distschedv1.JobState_JOB_STATE_QUEUED
	job.Attempt = 0
	job.AssignedWorkerId = ""
	job.LastError = ""
	job.FinishedAt = nil
	job.ScheduledAt = timestamppb.New(next)
	job.UpdatedAt = timestamppb.New(now)
	if err := s.putJob(job); err != nil {
		s.log.Error("cron reschedule: persist", "job_id", job.GetId(), "err", err)
		return false
	}
	s.armEnqueue(job.GetId(), time.Until(next))
	s.log.Info("cron job rescheduled", "job_id", job.GetId(), "next", next.Format(time.RFC3339))
	return true
}
