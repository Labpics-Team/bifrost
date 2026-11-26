package adaptive

import (
	"math"
	"time"
)

// The public EE contract defines the signals and their precedence, not its
// coefficients. Keep the fork's tuning private and in this one calculation.
const (
	recoveryInterval     = 30 * time.Second
	baselineDecay        = 0.02
	degradedLatencyRatio = 2
)

func (r *route) observeLatency(measured float64) {
	if measured <= 0 {
		return
	}
	if r.BaselineLatencyMS <= 0 {
		r.BaselineLatencyMS = r.LatencyMS
		if r.BaselineLatencyMS <= 0 {
			r.BaselineLatencyMS = measured
		}
	}
	if r.LatencyMS <= 0 {
		r.LatencyMS = measured
	} else {
		r.LatencyMS += decay * (measured - r.LatencyMS)
	}
	r.BaselineLatencyMS += baselineDecay * (measured - r.BaselineLatencyMS)
}

func (r *route) latencyRatio(peerBaseline float64) float64 {
	ratio := 1.0
	if r.BaselineLatencyMS > 0 {
		ratio = max(ratio, r.LatencyMS/r.BaselineLatencyMS)
	}
	if peerBaseline > 0 {
		ratio = max(ratio, r.LatencyMS/peerBaseline)
	}
	return ratio
}

func latencyFactor(ratio float64) float64 {
	return 0.5 + 0.5/math.Sqrt(max(1, ratio))
}

// A transient cooldown excludes traffic only until its deadline. Expiry is not
// a successful observation: the key stays recovering until real outcomes earn
// back its share, even when restored from an older checkpoint.
func (r *route) failed(now time.Time) bool {
	return r.FailedUntil.After(now)
}

func (r *route) recovering() bool {
	return !r.FailedUntil.IsZero() || r.Recovering && r.Recoveries < 5
}

func (r *route) score(peerBaseline float64) float64 {
	// Errors dominate substantial degradation. Latency is relative to both
	// the eligible peers and this key's own recent performance. Keep load
	// a smaller, bounded adjustment rather than letting long streams
	// eliminate an otherwise healthy key from rotation.
	w := math.Max(0.05, math.Pow(1-r.ErrorRate, 3))
	w *= latencyFactor(r.latencyRatio(peerBaseline))
	w *= 1 - 0.2*float64(r.Inflight)/(1+float64(r.Inflight))
	if r.recovering() {
		w *= 0.1 + 0.18*float64(r.Recoveries)
	}
	return w
}

func (c *Controller) keyWeightLocked(id routeID, now time.Time, peerBaseline float64) float64 {
	if c.holdLocked(id, now) != nil {
		return 0
	}
	r := c.routes[id]
	if r == nil {
		return 1
	}
	if r.failed(now) {
		return 0
	}
	return r.score(peerBaseline)
}

// recoveryDueLocked grants a stale unhealthy route one live observation through
// the existing probe lease. Ranking alone can otherwise starve a route after
// its first success, before it has earned a healthy score again.
func (c *Controller) recoveryDueLocked(r *route, now time.Time) bool {
	if r == nil || r.failed(now) || r.ProbeUntil.After(now) || c.holdLocked(r.routeID, now) != nil {
		return false
	}
	// Load affects the score, but a stalled physical request must not prevent
	// future observations after the current admission lease expires.
	return now.Sub(r.LastSeen) >= recoveryInterval &&
		(r.recovering() || r.ErrorRate > 0.05 || r.latencyRatio(0) >= degradedLatencyRatio)
}
