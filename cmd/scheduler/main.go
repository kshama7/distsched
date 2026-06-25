// Command scheduler runs a distsched scheduler node. In Milestone 1 it is a
// scaffold that reports build info; the gRPC server, BoltDB store, and priority
// queue land in Milestone 2.
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
		listen      = flag.String("listen", ":7070", "gRPC listen address")
		dataDir     = flag.String("data-dir", "./data", "directory for the BoltDB store")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("scheduler %s (%s)\n", buildinfo.Version, buildinfo.Commit)
		return
	}

	fmt.Fprintf(os.Stderr,
		"distsched scheduler %s: scaffold only. Server on %s with data-dir %s arrives in Milestone 2.\n",
		buildinfo.Version, *listen, *dataDir)
}
