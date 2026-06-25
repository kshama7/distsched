// Command schedulerctl is the operator CLI for distsched. It discovers the
// cluster leader from a seed list and submits/inspects jobs.
//
// Usage:
//
//	schedulerctl [--addr a,b,c] <command> [flags]
//
// Commands: submit, get, list, cancel, status, version.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
	"github.com/kshama7/distsched/internal/buildinfo"
	"github.com/kshama7/distsched/internal/ctl"
)

func main() {
	addr := flag.String("addr", "localhost:7070", "comma-separated scheduler addresses (seed list)")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	var seeds []string
	for _, s := range strings.Split(*addr, ",") {
		if s = strings.TrimSpace(s); s != "" {
			seeds = append(seeds, s)
		}
	}
	client := ctl.New(seeds)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var err error
	switch args[0] {
	case "submit":
		err = cmdSubmit(ctx, client, args[1:])
	case "get":
		err = cmdGet(ctx, client, args[1:])
	case "list":
		err = cmdList(ctx, client, args[1:])
	case "cancel":
		err = cmdCancel(ctx, client, args[1:])
	case "status":
		err = cmdStatus(ctx, client)
	case "version":
		fmt.Printf("schedulerctl %s (%s)\n", buildinfo.Version, buildinfo.Commit)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `schedulerctl — distsched operator CLI

Usage:
  schedulerctl [--addr a,b,c] <command> [flags]

Commands:
  submit   submit a job
  get      show a job by ID
  list     list jobs (optionally --state)
  cancel   cancel a job by ID
  status   show cluster leader, term, and workers
  version  print version
`)
}

// stringSlice collects repeatable flags.
type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }
func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func cmdSubmit(ctx context.Context, c *ctl.Client, argv []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	var args stringSlice
	id := fs.String("id", "", "explicit job ID (optional)")
	name := fs.String("name", "", "job name")
	command := fs.String("command", "", "command to run (required)")
	fs.Var(&args, "arg", "command argument (repeatable)")
	priority := fs.Int("priority", 0, "priority (higher runs first)")
	dependsOn := fs.String("depends-on", "", "comma-separated predecessor job IDs")
	cron := fs.String("cron", "", "cron expression for a recurring job")
	after := fs.Duration("after", 0, "delay before the job is eligible (e.g. 30s)")
	maxAttempts := fs.Int("max-attempts", 0, "max attempts before dead-lettering")
	_ = fs.Parse(argv)

	if *command == "" {
		return fmt.Errorf("--command is required")
	}
	var deps []string
	for _, d := range strings.Split(*dependsOn, ",") {
		if d = strings.TrimSpace(d); d != "" {
			deps = append(deps, d)
		}
	}

	jobID, err := c.Submit(ctx, ctl.SubmitParams{
		ID:          *id,
		Name:        *name,
		Command:     *command,
		Args:        args,
		Priority:    int32(*priority),
		DependsOn:   deps,
		Cron:        *cron,
		After:       *after,
		MaxAttempts: int32(*maxAttempts),
	})
	if err != nil {
		return err
	}
	fmt.Println(jobID)
	return nil
}

func cmdGet(ctx context.Context, c *ctl.Client, argv []string) error {
	if len(argv) != 1 {
		return fmt.Errorf("usage: get <job-id>")
	}
	job, err := c.Get(ctx, argv[0])
	if err != nil {
		return err
	}
	printJobDetail(job)
	return nil
}

func cmdList(ctx context.Context, c *ctl.Client, argv []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	state := fs.String("state", "", "filter by state (pending|queued|running|succeeded|failed|canceled)")
	_ = fs.Parse(argv)

	jobs, err := c.List(ctx, parseState(*state))
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tPRIORITY\tSTATE\tATTEMPT\tDEPS")
	for _, j := range jobs {
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%d\t%d\n",
			j.GetId(), j.GetName(), j.GetPriority(), shortState(j.GetState()), j.GetAttempt(), len(j.GetDependsOn()))
	}
	return w.Flush()
}

func cmdCancel(ctx context.Context, c *ctl.Client, argv []string) error {
	if len(argv) != 1 {
		return fmt.Errorf("usage: cancel <job-id>")
	}
	job, err := c.Cancel(ctx, argv[0])
	if err != nil {
		return err
	}
	fmt.Printf("%s -> %s\n", job.GetId(), shortState(job.GetState()))
	return nil
}

func cmdStatus(ctx context.Context, c *ctl.Client) error {
	st, err := c.Status(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("leader: %s  term: %d\n\n", st.GetLease().GetHolderId(), st.GetLease().GetTerm())

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "SCHEDULER\tADDRESS\tROLE")
	for _, n := range st.GetSchedulers() {
		fmt.Fprintf(w, "%s\t%s\t%s\n", n.GetId(), n.GetAddress(), shortRole(n.GetRole()))
	}
	_ = w.Flush()

	fmt.Println()
	w = tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "WORKER\tADDRESS\tSTATE\tRUNNING")
	for _, wk := range st.GetWorkers() {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", wk.GetId(), wk.GetAddress(), shortWorkerState(wk.GetState()), wk.GetRunningTasks())
	}
	return w.Flush()
}

func printJobDetail(j *distschedv1.Job) {
	fmt.Printf("id:        %s\n", j.GetId())
	fmt.Printf("name:      %s\n", j.GetName())
	fmt.Printf("state:     %s\n", shortState(j.GetState()))
	fmt.Printf("priority:  %d\n", j.GetPriority())
	fmt.Printf("attempt:   %d\n", j.GetAttempt())
	fmt.Printf("command:   %s %s\n", j.GetSpec().GetCommand(), strings.Join(j.GetSpec().GetArgs(), " "))
	if len(j.GetDependsOn()) > 0 {
		fmt.Printf("dependsOn: %s\n", strings.Join(j.GetDependsOn(), ", "))
	}
	if cron := j.GetSchedule().GetCronExpr(); cron != "" {
		fmt.Printf("cron:      %s\n", cron)
	}
	if j.GetLastError() != "" {
		fmt.Printf("lastError: %s\n", j.GetLastError())
	}
}

func parseState(s string) distschedv1.JobState {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "pending":
		return distschedv1.JobState_JOB_STATE_PENDING
	case "queued":
		return distschedv1.JobState_JOB_STATE_QUEUED
	case "running":
		return distschedv1.JobState_JOB_STATE_RUNNING
	case "succeeded":
		return distschedv1.JobState_JOB_STATE_SUCCEEDED
	case "failed":
		return distschedv1.JobState_JOB_STATE_FAILED
	case "canceled":
		return distschedv1.JobState_JOB_STATE_CANCELED
	default:
		return distschedv1.JobState_JOB_STATE_UNSPECIFIED
	}
}

func shortState(s distschedv1.JobState) string {
	return strings.TrimPrefix(s.String(), "JOB_STATE_")
}
func shortRole(r distschedv1.SchedulerRole) string {
	return strings.TrimPrefix(r.String(), "SCHEDULER_ROLE_")
}
func shortWorkerState(s distschedv1.WorkerState) string {
	return strings.TrimPrefix(s.String(), "WORKER_STATE_")
}
