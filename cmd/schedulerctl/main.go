// Command schedulerctl is the operator CLI for distsched. In Milestone 1 it is
// a scaffold; submit/get/list/cancel and cluster status land in Milestone 7.
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
		addr        = flag.String("addr", "localhost:7070", "scheduler gRPC address")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("schedulerctl %s (%s)\n", buildinfo.Version, buildinfo.Commit)
		return
	}

	fmt.Fprintf(os.Stderr,
		"distsched schedulerctl %s: scaffold only. Subcommands (submit/get/list/cancel/status against %s) arrive in Milestone 7.\n",
		buildinfo.Version, *addr)
}
