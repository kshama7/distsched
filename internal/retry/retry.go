// Package retry computes exponential-backoff delays for failed jobs.
package retry

import (
	"math/rand"
	"time"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

// Defaults applied when a job has no RetryPolicy or leaves fields zero.
const (
	DefaultMaxAttempts = 3
	defaultInitial     = time.Second
	defaultMax         = 30 * time.Second
	defaultMultiplier  = 2.0
	defaultJitter      = 0.2
)

// MaxAttempts returns the total number of attempts permitted (including the
// first), defaulting when the policy is absent or non-positive.
func MaxAttempts(p *distschedv1.RetryPolicy) int {
	if p == nil || p.GetMaxAttempts() <= 0 {
		return DefaultMaxAttempts
	}
	return int(p.GetMaxAttempts())
}

// Backoff returns the delay before retrying after the given attempt failed.
// attempt is 1-indexed: Backoff(p, 1) is the wait after the first attempt.
//
//	delay = min(max_backoff, initial · multiplier^(attempt-1)) · (1 ± jitter)
func Backoff(p *distschedv1.RetryPolicy, attempt int) time.Duration {
	initial, maxB, mult, jitter := defaultInitial, defaultMax, defaultMultiplier, defaultJitter
	if p != nil {
		if d := p.GetInitialBackoff().AsDuration(); d > 0 {
			initial = d
		}
		if d := p.GetMaxBackoff().AsDuration(); d > 0 {
			maxB = d
		}
		if p.GetBackoffMultiplier() > 0 {
			mult = p.GetBackoffMultiplier()
		}
		if p.GetJitter() > 0 {
			jitter = p.GetJitter()
		}
	}
	if attempt < 1 {
		attempt = 1
	}

	base := float64(initial)
	for i := 1; i < attempt; i++ {
		base *= mult
		if base >= float64(maxB) {
			base = float64(maxB)
			break
		}
	}

	if jitter > 0 {
		base *= 1 + (rand.Float64()*2-1)*jitter // #nosec G404 -- jitter, not security
	}
	if base < 0 {
		base = 0
	}
	return time.Duration(base)
}
