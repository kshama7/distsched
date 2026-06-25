package worker

import (
	"bytes"
	"context"
	"os"
	"os/exec"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

// maxOutputBytes caps captured task output to keep reports small.
const maxOutputBytes = 8 << 10 // 8 KiB

// execute runs a TaskSpec as a subprocess and reports the outcome. A non-zero
// exit (or timeout) is a FAILED result; the error text and captured output are
// returned for the dead-letter view.
func execute(ctx context.Context, spec *distschedv1.TaskSpec) (distschedv1.TaskResult, string, string) {
	if spec.GetCommand() == "" {
		return distschedv1.TaskResult_TASK_RESULT_FAILED, "", "empty command"
	}
	if to := spec.GetTimeout().AsDuration(); to > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, to)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, spec.GetCommand(), spec.GetArgs()...)
	if env := spec.GetEnv(); len(env) > 0 {
		cmd.Env = os.Environ()
		for k, v := range env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	err := cmd.Run()
	out := truncate(buf.String())
	if err != nil {
		return distschedv1.TaskResult_TASK_RESULT_FAILED, out, err.Error()
	}
	return distschedv1.TaskResult_TASK_RESULT_SUCCEEDED, out, ""
}

func truncate(s string) string {
	if len(s) > maxOutputBytes {
		return s[:maxOutputBytes] + "...(truncated)"
	}
	return s
}
