// Package fillrequest implements the persisted refill demand queue.
//
// Low level probes do not start fills directly anymore. They submit a
// Demand to the Queue, which deduplicates per vessel and journals every
// request/claim/cancel transition. The Scheduler dispatches queued
// demands only when a compatible circuit is available, ordering by
// urgency and accumulated wait time so low priority demands cannot starve.
package fillrequest

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Urgency of a refill demand. Higher urgency sorts first, but wait time
// aging (see Priority) eventually promotes any waiting demand.
type Urgency int

const (
	UrgencyNormal   Urgency = 0
	UrgencyHigh     Urgency = 1
	UrgencyCritical Urgency = 2
)

func (u Urgency) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

func (u *Urgency) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	parsed, ok := parseUrgency(text)
	if !ok {
		return errors.New("invalid demand urgency: " + text)
	}
	*u = parsed
	return nil
}

func (u Urgency) valid() bool {
	switch u {
	case UrgencyNormal, UrgencyHigh, UrgencyCritical:
		return true
	default:
		return false
	}
}

func (u Urgency) String() string {
	switch u {
	case UrgencyHigh:
		return "high"
	case UrgencyCritical:
		return "critical"
	default:
		return "normal"
	}
}

// parseUrgency mirrors Urgency.String without leaking encoding helpers.
func parseUrgency(value string) (Urgency, bool) {
	switch strings.TrimSpace(value) {
	case "normal", "":
		return UrgencyNormal, true
	case "high":
		return UrgencyHigh, true
	case "critical":
		return UrgencyCritical, true
	default:
		return UrgencyNormal, false
	}
}

// DemandState is the lifecycle state of a queued refill demand.
type DemandState string

const (
	// StateQueued: demand recorded and waiting for a compatible circuit.
	StateQueued DemandState = "queued"
	// StateDispatched: claimed by the scheduler; a fill session was created.
	StateDispatched DemandState = "dispatched"
	// StateFulfilled: the linked fill session reached a terminal state.
	StateFulfilled DemandState = "fulfilled"
	// StateCancelled: the demand was retracted before it could complete.
	StateCancelled DemandState = "cancelled"
)

func (s DemandState) terminal() bool {
	return s == StateFulfilled || s == StateCancelled
}

// Demand records a single per-vessel refill request and its scheduling
// bookkeeping. Exactly one live (queued/dispatched) demand may exist per
// vessel at any time.
type Demand struct {
	VesselID     string      `json:"vessel_id"`
	Urgency      Urgency     `json:"urgency"`
	State        DemandState `json:"state"`
	Liters       float64     `json:"liters"`
	RequestedAt  time.Time   `json:"requested_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
	DispatchedAt time.Time   `json:"dispatched_at,omitempty"`
	CancelledAt  time.Time   `json:"cancelled_at,omitempty"`
	CancelReason string      `json:"cancel_reason,omitempty"`
	SessionID    string      `json:"session_id,omitempty"`
	// Sequence of the journal event that created the demand; used as a
	// tie breaker so earlier submissions win among equal priorities.
	Sequence uint64 `json:"sequence,omitempty"`
}

// Priority expresses how urgently a queued demand should be claimed. It
// combines explicit urgency with wait-time aging: every AgingStep of
// waiting adds one effective urgency level, capped at UrgencyCritical.
// A normal demand that has waited long enough therefore ties with a
// freshly arrived critical demand, and wins on RequestedAt — no demand
// can starve forever.
func (d Demand) Priority(now time.Time) Urgency {
	if d.State != StateQueued {
		return d.Urgency
	}
	levels := 0
	if d.RequestedAt.Before(now) {
		levels = int(now.Sub(d.RequestedAt) / AgingStep)
	}
	promoted := int(d.Urgency) + levels
	if promoted > int(UrgencyCritical) {
		promoted = int(UrgencyCritical)
	}
	return Urgency(promoted)
}

// Order reports whether demand a should be claimed before demand b.
// Queued demands are ranked by aged priority, then by earliest request
// (oldest wait first), then by submission sequence for determinism.
func Order(a, b Demand, now time.Time) bool {
	pa, pb := a.Priority(now), b.Priority(now)
	if pa != pb {
		return pa > pb
	}
	if !a.RequestedAt.Equal(b.RequestedAt) {
		return a.RequestedAt.Before(b.RequestedAt)
	}
	return a.Sequence < b.Sequence
}

func newDemand(vesselID string, urgency Urgency, liters float64, now time.Time) (Demand, error) {
	vesselID = strings.TrimSpace(vesselID)
	if vesselID == "" {
		return Demand{}, errors.New("vessel id is required")
	}
	if !urgency.valid() {
		return Demand{}, errors.New("invalid demand urgency")
	}
	if liters <= 0 {
		return Demand{}, errors.New("demand liters must be positive")
	}
	return Demand{
		VesselID:    vesselID,
		Urgency:     urgency,
		State:       StateQueued,
		Liters:      liters,
		RequestedAt: now.UTC(),
		UpdatedAt:   now.UTC(),
	}, nil
}

func (d Demand) claimed(sessionID string, now time.Time) (Demand, error) {
	if d.State != StateQueued {
		return Demand{}, errors.New("demand is not queued")
	}
	if strings.TrimSpace(sessionID) == "" {
		return Demand{}, errors.New("session id is required to claim a demand")
	}
	d.State = StateDispatched
	d.SessionID = sessionID
	d.DispatchedAt = now.UTC()
	d.UpdatedAt = now.UTC()
	return d, nil
}

// leased moves a queued demand into dispatched without a session yet.
// It is an in-memory reservation held for the duration of starting the
// fill session, so concurrent dispatch passes cannot pick the same
// demand twice.
func (d Demand) leased(now time.Time) (Demand, error) {
	if d.State != StateQueued {
		return Demand{}, errors.New("demand is not queued")
	}
	d.State = StateDispatched
	d.UpdatedAt = now.UTC()
	return d, nil
}

// bound attaches the started session to a leased demand.
func (d Demand) bound(sessionID string, now time.Time) (Demand, error) {
	if d.State != StateDispatched || d.SessionID != "" {
		return Demand{}, errors.New("demand is not awaiting a session binding")
	}
	if strings.TrimSpace(sessionID) == "" {
		return Demand{}, errors.New("session id is required to bind a demand")
	}
	d.SessionID = sessionID
	d.DispatchedAt = now.UTC()
	d.UpdatedAt = now.UTC()
	return d, nil
}

// released returns an unbound leased demand to the queue.
func (d Demand) released(now time.Time) (Demand, error) {
	if d.State != StateDispatched || d.SessionID != "" {
		return Demand{}, errors.New("only unbound leased demands can be released")
	}
	d.State = StateQueued
	d.UpdatedAt = now.UTC()
	return d, nil
}

func (d Demand) fulfilled(now time.Time) (Demand, error) {
	if d.State != StateDispatched {
		return Demand{}, errors.New("demand was not dispatched")
	}
	d.State = StateFulfilled
	d.UpdatedAt = now.UTC()
	return d, nil
}

func (d Demand) cancelled(reason string, now time.Time) (Demand, error) {
	if d.State.terminal() {
		return Demand{}, errors.New("demand already finished")
	}
	if strings.TrimSpace(reason) == "" {
		return Demand{}, errors.New("cancel reason is required")
	}
	d.State = StateCancelled
	d.CancelReason = reason
	d.CancelledAt = now.UTC()
	d.UpdatedAt = now.UTC()
	return d, nil
}

// requeued returns a dispatched demand to the queue after its in-flight
// session can no longer be tracked (for example a process restart). The
// original RequestedAt is preserved so accumulated wait time — and the
// aging priority it earned — is not lost.
func (d Demand) requeued(now time.Time) (Demand, error) {
	if d.State != StateDispatched {
		return Demand{}, errors.New("only dispatched demands can be requeued")
	}
	d.State = StateQueued
	d.SessionID = ""
	d.DispatchedAt = time.Time{}
	d.UpdatedAt = now.UTC()
	return d, nil
}
