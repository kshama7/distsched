// Package ids generates unique, human-readable identifiers.
package ids

import "github.com/google/uuid"

// New returns a prefixed UUIDv4, e.g. New("job") -> "job-3f2a...".
func New(prefix string) string {
	return prefix + "-" + uuid.NewString()
}
