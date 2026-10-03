package fillrequest

import "time"

// AgingStep is how long a queued demand waits before it gains one
// effective urgency level. After 2*AgingStep even a normal demand is
// scheduled at critical priority, which bounds how long any demand can
// be overtaken by newer, more urgent arrivals.
const AgingStep = 2 * time.Minute

// DispatchOutcome is written back by the scheduler when a claimed
// demand's fill session reaches a terminal state.
type DispatchOutcome int

const (
	// OutcomeFulfilled marks the demand as served by the session.
	OutcomeFulfilled DispatchOutcome = iota
	// OutcomeCancelled releases the demand after a failed/aborted session.
	OutcomeCancelled
)
