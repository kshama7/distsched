// Package ctl is the client library behind the schedulerctl CLI: it discovers
// the cluster leader and issues JobService / SchedulerService RPCs.
package ctl

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

// Client talks to a distsched cluster given a seed list of scheduler addresses.
type Client struct {
	seeds []string
}

// New constructs a Client.
func New(seeds []string) *Client { return &Client{seeds: seeds} }

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// Status returns cluster status from the first reachable seed.
func (c *Client) Status(ctx context.Context) (*distschedv1.GetClusterStatusResponse, error) {
	var lastErr error
	for _, seed := range c.seeds {
		conn, err := dial(seed)
		if err != nil {
			lastErr = err
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		resp, err := distschedv1.NewSchedulerServiceClient(conn).GetClusterStatus(cctx, &distschedv1.GetClusterStatusRequest{})
		cancel()
		_ = conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no scheduler reachable among %v", c.seeds)
}

// leaderConn discovers the leader and returns a connection to it.
func (c *Client) leaderConn(ctx context.Context) (*grpc.ClientConn, error) {
	status, err := c.Status(ctx)
	if err != nil {
		return nil, err
	}
	holder := status.GetLease().GetHolderId()
	for _, n := range status.GetSchedulers() {
		if n.GetId() == holder {
			return dial(n.GetAddress())
		}
	}
	return nil, fmt.Errorf("no leader currently elected")
}

// SubmitParams describes a job to submit.
type SubmitParams struct {
	ID          string
	Name        string
	Command     string
	Args        []string
	Priority    int32
	DependsOn   []string
	Cron        string
	After       time.Duration // if >0, run_at = now + After
	MaxAttempts int32
}

// Submit creates a job on the leader and returns its ID.
func (c *Client) Submit(ctx context.Context, p SubmitParams) (string, error) {
	conn, err := c.leaderConn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	job := &distschedv1.Job{
		Id:        p.ID,
		Name:      p.Name,
		Priority:  p.Priority,
		DependsOn: p.DependsOn,
		Spec:      &distschedv1.TaskSpec{Command: p.Command, Args: p.Args},
	}
	if p.MaxAttempts > 0 {
		job.RetryPolicy = &distschedv1.RetryPolicy{MaxAttempts: p.MaxAttempts}
	}
	if p.Cron != "" {
		job.Schedule = &distschedv1.Schedule{Kind: &distschedv1.Schedule_CronExpr{CronExpr: p.Cron}}
	} else if p.After > 0 {
		job.Schedule = &distschedv1.Schedule{Kind: &distschedv1.Schedule_RunAt{RunAt: timestamppb.New(time.Now().Add(p.After))}}
	}

	resp, err := distschedv1.NewJobServiceClient(conn).SubmitJob(ctx, &distschedv1.SubmitJobRequest{Job: job})
	if err != nil {
		return "", err
	}
	return resp.GetJobId(), nil
}

// Get returns a job by ID.
func (c *Client) Get(ctx context.Context, id string) (*distschedv1.Job, error) {
	conn, err := c.leaderConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	resp, err := distschedv1.NewJobServiceClient(conn).GetJob(ctx, &distschedv1.GetJobRequest{JobId: id})
	if err != nil {
		return nil, err
	}
	return resp.GetJob(), nil
}

// List returns jobs, optionally filtered by state.
func (c *Client) List(ctx context.Context, state distschedv1.JobState) ([]*distschedv1.Job, error) {
	conn, err := c.leaderConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	resp, err := distschedv1.NewJobServiceClient(conn).ListJobs(ctx, &distschedv1.ListJobsRequest{StateFilter: state})
	if err != nil {
		return nil, err
	}
	return resp.GetJobs(), nil
}

// Cancel cancels a job by ID and returns its updated record.
func (c *Client) Cancel(ctx context.Context, id string) (*distschedv1.Job, error) {
	conn, err := c.leaderConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	resp, err := distschedv1.NewJobServiceClient(conn).CancelJob(ctx, &distschedv1.CancelJobRequest{JobId: id})
	if err != nil {
		return nil, err
	}
	return resp.GetJob(), nil
}
