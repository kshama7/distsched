// Package buildinfo exposes version metadata, overridable at link time via
// -ldflags "-X github.com/kshama7/distsched/internal/buildinfo.Version=...".
package buildinfo

// Version is the build version; "dev" for local builds.
var Version = "dev"

// Commit is the git short SHA the binary was built from.
var Commit = "none"
