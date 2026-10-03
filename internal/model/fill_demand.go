package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type DemandState string

const (
	DemandQueued     DemandState = "queued"
	DemandDispatched DemandState = "dispatched"
	DemandFulfilled  DemandState = "fulfilled"
	DemandCancelled  DemandState = "cancelled"
)

// FillDemandUrgency ranks how urgently a vessel needs a refill. A lower
// numeric value means a more urgent demand.
type FillDemandUrgency string

const (
	UrgencyCritical FillDemandUrgency = "critical"
	UrgencyHigh     FillDemandUrgency = "high"
	UrgencyNormal   FillDemandUrgency = "normal"
)

// FillDemand is a queued refill request for one vessel. At most one open
// (queued or dispatched) demand exists per vessel at any time, which together
// with the coordinator's per-vessel active-session guard guarantees the same
// valve route is never opened twice concurrently for one vessel.
type FillDemand struct {
	ID            uuid.UUID         `json:"id"`
	VesselID      string            `json:"vessel_id"`
	State         DemandState       `json:"state"`
	Urgency       FillDemandUrgency `json:"urgency"`
	PlannedLiters float64           `json:"planned_liters"`
	Source        FillOrigin        `json:"source"`
	LevelMM       float64           `json:"level_mm,omitempty"`
	ReceivedAt    time.Time         `json:"received_at"`
	LastSignalAt  time.Time         `json:"last_signal_at"`
	// Attempts and NotBefore throttle repeated dispatch attempts whose start
	// fails. They are intentionally not persisted: a restart re-evaluates the
	// demand immediately against fresh in-memory circuit state.
	Attempts     int       `json:"-"`
	NotBefore    time.Time `json:"-"`
	DispatchedAt time.Time `json:"dispatched_at,omitempty"`
	SessionID    uuid.UUID `json:"session_id,omitempty"`
	CancelledAt  time.Time `json:"cancelled_at,omitempty"`
	CancelReason string    `json:"cancel_reason,omitempty"`
}

func NewFillRequest(vesselID string, urgency FillDemandUrgency, liters float64, source FillOrigin, levelMM float64, now time.Time) (FillDemand, error) {
	if vesselID == "" {
		return FillDemand{}, errors.New("demand vessel id is required")
	}
	if liters <= 0 {
		return FillDemand{}, errors.New("demand planned volume must be positive")
	}
	if source != FillAutomatic && source != FillManual {
		return FillDemand{}, errors.New("invalid demand source")
	}
	if !urgency.Valid() {
		return FillDemand{}, errors.New("invalid demand urgency")
	}
	stamp := now.UTC()
	return FillDemand{
		ID:            uuid.New(),
		VesselID:      vesselID,
		State:         DemandQueued,
		Urgency:       urgency,
		PlannedLiters: liters,
		Source:        source,
		LevelMM:       levelMM,
		ReceivedAt:    stamp,
		LastSignalAt:  stamp,
	}, nil
}

func (u FillDemandUrgency) Valid() bool {
	switch u {
	case UrgencyCritical, UrgencyHigh, UrgencyNormal:
		return true
	default:
		return false
	}
}

// Rank orders urgency bands; a higher rank is scheduled first.
func (u FillDemandUrgency) Rank() int {
	switch u {
	case UrgencyCritical:
		return 3
	case UrgencyHigh:
		return 2
	default:
		return 1
	}
}

// Open reports whether the demand still occupies its vessel's single
// demand slot: waiting for a circuit or already bound to a live session.
func (d FillDemand) Open() bool {
	return d.State == DemandQueued || d.State == DemandDispatched
}

// Waiting reports whether the demand is queued for a free circuit.
func (d FillDemand) Waiting() bool {
	return d.State == DemandQueued
}

// NoteSignal records a repeated low-level observation. Repeated signals refresh
// the urgency (a vessel that keeps reporting low level may have grown more
// urgent) but never reset the original arrival time, so wait-time fairness is
// preserved for anti-starvation scheduling.
func (d FillDemand) NoteSignal(urgency FillDemandUrgency, levelMM float64, now time.Time) FillDemand {
	if urgency.Valid() && urgency.Rank() > d.Urgency.Rank() {
		d.Urgency = urgency
	}
	d.LevelMM = levelMM
	d.LastSignalAt = now.UTC()
	return d
}

// Claim binds the demand to a fill session once a compatible circuit is free.
func (d FillDemand) Claim(sessionID uuid.UUID, now time.Time) (FillDemand, error) {
	if d.State != DemandQueued {
		return FillDemand{}, errors.New("demand is not waiting to be dispatched")
	}
	if sessionID == uuid.Nil {
		return FillDemand{}, errors.New("dispatch session id is required")
	}
	d.State = DemandDispatched
	d.SessionID = sessionID
	d.DispatchedAt = now.UTC()
	return d, nil
}

// Fulfill marks a dispatched demand complete after its fill session finishes
// successfully.
func (d FillDemand) Fulfill(now time.Time) (FillDemand, error) {
	if d.State != DemandDispatched {
		return FillDemand{}, errors.New("demand has not been dispatched")
	}
	d.State = DemandFulfilled
	return d, nil
}

// RequeueForRestart returns the demand ready to be scheduled again after a
// process restart. In-memory circuit reservations and valve routes do not
// survive a restart, so a dispatched demand cannot trust its old session: the
// slot is released back to the queue. Queued demands keep their identity and
// original arrival time so wait-based fairness survives the restart.
func (d FillDemand) RequeueForRestart() (FillDemand, error) {
	if d.State != DemandDispatched {
		return FillDemand{}, errors.New("only dispatched demands can be requeued")
	}
	d.State = DemandQueued
	d.SessionID = uuid.Nil
	d.DispatchedAt = time.Time{}
	return d, nil
}

// Requeue returns the demand to the queue after its fill session ended without
// fulfilling it (abort or valve failure). The original arrival time is kept,
// so the demand keeps its accumulated wait-time priority and is not penalized
// for the failed attempt.
func (d FillDemand) Requeue(reason string, now time.Time) (FillDemand, error) {
	if d.State != DemandDispatched {
		return FillDemand{}, errors.New("only dispatched demands can be requeued")
	}
	if reason == "" {
		return FillDemand{}, errors.New("requeue reason is required")
	}
	d.State = DemandQueued
	d.SessionID = uuid.Nil
	d.DispatchedAt = time.Time{}
	d.CancelReason = ""
	d.Attempts = 0
	d.NotBefore = time.Time{}
	return d, nil
}

func (d FillDemand) Cancel(reason string, now time.Time) (FillDemand, error) {
	if !d.Open() {
		return FillDemand{}, errors.New("demand is already closed")
	}
	if reason == "" {
		return FillDemand{}, errors.New("cancel reason is required")
	}
	d.State = DemandCancelled
	d.CancelReason = reason
	d.CancelledAt = now.UTC()
	return d, nil
}
