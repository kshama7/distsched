// Command worker runs a distsched worker node. In Milestone 1 it is a scaffold
// that reports build info; registration, heartbeat, and the task-execution loop
// land in Milestones 2-3.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/kshama7/distsched/internal/buildinfo"
)

func main() {
	var (
		showVersion = flag.Bool("version", false, "print version and exit")
		scheduler   = flag.String("scheduler", "localhost:7070", "scheduler gRPC address")
		capacity    = flag.Int("capacity", 4, "max concurrent tasks")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("worker %s (%s)\n", buildinfo.Version, buildinfo.Commit)
		return
	}

	fmt.Fprintf(os.Stderr,
		"distsched worker %s: scaffold only. Will register with %s (capacity %d) in Milestone 2.\n",
		buildinfo.Version, *scheduler, *capacity)
}
