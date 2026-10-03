package fillrequest

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

// EventAppender is the journal sink used by the queue. *journal.Store
// satisfies it.
type EventAppender interface {
	Append(model.Event) (model.Event, error)
}

// Journal partition and event kinds used for demand lifecycle tracking.
const (
	Partition = "fill-requests"

	EventRequested  = "fill_request.requested"
	EventDispatched = "fill_request.dispatched"
	EventFulfilled  = "fill_request.fulfilled"
	EventCancelled  = "fill_request.cancelled"
	EventRequeued   = "fill_request.requeued"
)

var (
	// ErrActiveDemand is returned when a vessel already has a queued or
	// dispatched demand; repeated low level probes are deduplicated.
	ErrActiveDemand = errors.New("vessel already has an active refill demand")
	// ErrNoDemand is returned when no live demand exists for a vessel.
	ErrNoDemand = errors.New("no active refill demand for vessel")
)

// Queue collects per-vessel refill demands, deduplicates them, and
// journals every request/claim/fulfil/cancel transition so state can be
// reconstructed after a restart.
type Queue struct {
	mu      sync.RWMutex
	demands map[string]Demand
	journal EventAppender
	now     func() time.Time
}

func NewQueue(journal EventAppender, now func() time.Time) (*Queue, error) {
	if journal == nil {
		return nil, errors.New("demand queue journal is required")
	}
	if now == nil {
		now = time.Now
	}
	return &Queue{demands: make(map[string]Demand), journal: journal, now: now}, nil
}

// Request records a refill demand for a vessel. A repeated request while
// the vessel already has a queued or dispatched demand is deduplicated:
// the existing demand is returned together with ErrActiveDemand and no
// new event is appended.
func (q *Queue) Request(vesselID string, urgency Urgency, liters float64) (Demand, error) {
	demand, err := newDemand(vesselID, urgency, liters, q.now())
	if err != nil {
		return Demand{}, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if existing, ok := q.demands[demand.VesselID]; ok && !existing.State.terminal() {
		return existing, ErrActiveDemand
	}
	persisted, err := q.appendLocked(EventRequested, demand.VesselID, requestedPayload{
		VesselID:    demand.VesselID,
		Urgency:     demand.Urgency,
		Liters:      demand.Liters,
		RequestedAt: demand.RequestedAt,
	})
	if err != nil {
		return Demand{}, err
	}
	demand.Sequence = persisted.Sequence
	q.demands[demand.VesselID] = demand
	return demand, nil
}

// Lease reserves a queued demand for the duration of starting its fill
// session. The reservation is in-memory only (not yet journaled): the
// demand moves to dispatched with no session, so a concurrent pass or a
// duplicate probe cannot claim it. Bind must follow on success;
// ReleaseLease must follow on failure.
func (q *Queue) Lease(vesselID string) (Demand, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	demand, ok := q.demands[vesselID]
	if !ok || demand.State.terminal() {
		return Demand{}, ErrNoDemand
	}
	if demand.State == StateDispatched {
		return demand, ErrActiveDemand
	}
	next, err := demand.leased(q.now())
	if err != nil {
		return Demand{}, err
	}
	q.demands[vesselID] = next
	return next, nil
}

// Bind attaches the started fill session to a leased demand and
// journals the dispatch. On journal failure the caller must abort the
// just-started session and call ReleaseLease, so no session is ever
// left untracked.
func (q *Queue) Bind(vesselID, sessionID string) (Demand, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	demand, ok := q.demands[vesselID]
	if !ok {
		return Demand{}, ErrNoDemand
	}
	next, err := demand.bound(sessionID, q.now())
	if err != nil {
		return Demand{}, err
	}
	if _, err := q.appendLocked(EventDispatched, vesselID, dispatchPayload{
		VesselID:     vesselID,
		SessionID:    sessionID,
		DispatchedAt: next.DispatchedAt,
	}); err != nil {
		return Demand{}, err
	}
	q.demands[vesselID] = next
	return next, nil
}

// ReleaseLease returns an unbound leased demand to the queue after the
// fill session failed to start.
func (q *Queue) ReleaseLease(vesselID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	demand, ok := q.demands[vesselID]
	if !ok {
		return ErrNoDemand
	}
	next, err := demand.released(q.now())
	if err != nil {
		return err
	}
	q.demands[vesselID] = next
	return nil
}

// Resolve closes a dispatched demand after its fill session ends.
func (q *Queue) Resolve(vesselID string, outcome DispatchOutcome, reason string) (Demand, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	demand, ok := q.demands[vesselID]
	if !ok || demand.State.terminal() {
		return Demand{}, ErrNoDemand
	}
	now := q.now()
	var (
		next    Demand
		err     error
		kind    string
		payload any
	)
	switch outcome {
	case OutcomeFulfilled:
		next, err = demand.fulfilled(now)
		kind = EventFulfilled
		payload = resolvePayload{VesselID: vesselID, SessionID: demand.SessionID, At: next.UpdatedAt}
	default:
		next, err = demand.cancelled(reason, now)
		kind = EventCancelled
		payload = cancelPayload{
			VesselID:  vesselID,
			SessionID: demand.SessionID,
			Reason:    reason,
			At:        next.CancelledAt,
		}
	}
	if err != nil {
		return Demand{}, err
	}
	if _, err := q.appendLocked(kind, vesselID, payload); err != nil {
		return Demand{}, err
	}
	q.demands[vesselID] = next
	return next, nil
}

// ResolveBySession closes the dispatched demand linked to sessionID. It
// is invoked from the fill session finalizer. A missing demand is not an
// error (manual sessions have no demand), so callers get ok=false.
func (q *Queue) ResolveBySession(sessionID string, outcome DispatchOutcome, reason string) (Demand, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for vesselID, demand := range q.demands {
		if demand.State != StateDispatched || demand.SessionID != sessionID {
			continue
		}
		now := q.now()
		var (
			next    Demand
			err     error
			kind    string
			payload any
		)
		switch outcome {
		case OutcomeFulfilled:
			next, err = demand.fulfilled(now)
			kind = EventFulfilled
			payload = resolvePayload{VesselID: vesselID, SessionID: sessionID, At: next.UpdatedAt}
		default:
			next, err = demand.cancelled(reason, now)
			kind = EventCancelled
			payload = cancelPayload{VesselID: vesselID, SessionID: sessionID, Reason: reason, At: next.CancelledAt}
		}
		if err != nil {
			return Demand{}, false, err
		}
		if _, err := q.appendLocked(kind, vesselID, payload); err != nil {
			return Demand{}, false, err
		}
		q.demands[vesselID] = next
		return next, true, nil
	}
	return Demand{}, false, nil
}

// RequeueDispatched returns dispatched demands whose sessions cannot be
// tracked after a restart back to the queued state, preserving their
// original request time. It is a no-op for demands whose session is
// present in liveSessions.
func (q *Queue) RequeueDispatched(liveSessions map[string]bool) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	requeued := 0
	for vesselID, demand := range q.demands {
		if demand.State != StateDispatched || liveSessions[demand.SessionID] {
			continue
		}
		next, err := demand.requeued(q.now())
		if err != nil {
			return requeued, err
		}
		if _, err := q.appendLocked(EventRequeued, vesselID, requeuePayload{
			VesselID:  vesselID,
			SessionID: demand.SessionID,
			At:        next.UpdatedAt,
		}); err != nil {
			return requeued, err
		}
		q.demands[vesselID] = next
		requeued++
	}
	return requeued, nil
}

// Cancel retracts a queued demand. A dispatched demand has an open fill
// session and must be aborted through the fill service instead, so it is
// rejected here to avoid losing track of an open valve route.
func (q *Queue) Cancel(vesselID, reason string) (Demand, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	demand, ok := q.demands[vesselID]
	if !ok || demand.State.terminal() {
		return Demand{}, ErrNoDemand
	}
	if reason == "" {
		return Demand{}, errors.New("cancel reason is required")
	}
	if demand.State == StateDispatched {
		return Demand{}, errors.New("demand is dispatched; abort the fill session instead")
	}
	next, err := demand.cancelled(reason, q.now())
	if err != nil {
		return Demand{}, err
	}
	if _, err := q.appendLocked(EventCancelled, vesselID, cancelPayload{
		VesselID: vesselID,
		Reason:   reason,
		At:       next.CancelledAt,
	}); err != nil {
		return Demand{}, err
	}
	q.demands[vesselID] = next
	return next, nil
}

// Get returns the latest demand recorded for a vessel.
func (q *Queue) Get(vesselID string) (Demand, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	demand, ok := q.demands[vesselID]
	return demand, ok
}

// Queued returns demands still waiting to be claimed, in dispatch order
// (aged priority first, then longest wait).
func (q *Queue) Queued() []Demand {
	now := q.now()
	q.mu.RLock()
	result := make([]Demand, 0)
	for _, demand := range q.demands {
		if demand.State == StateQueued {
			result = append(result, demand)
		}
	}
	q.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return Order(result[i], result[j], now) })
	return result
}

// Dispatched returns vessel -> session for demands with a live session.
// Used during recovery to reattach session finalization callbacks.
func (q *Queue) Dispatched() []Demand {
	q.mu.RLock()
	defer q.mu.RUnlock()
	result := make([]Demand, 0)
	for _, demand := range q.demands {
		if demand.State == StateDispatched {
			result = append(result, demand)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].VesselID < result[j].VesselID })
	return result
}

// List returns every recorded demand, including terminal ones, ordered
// by submission sequence for observability.
func (q *Queue) List() []Demand {
	q.mu.RLock()
	defer q.mu.RUnlock()
	result := make([]Demand, 0, len(q.demands))
	for _, demand := range q.demands {
		result = append(result, demand)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Sequence != result[j].Sequence {
			return result[i].Sequence < result[j].Sequence
		}
		return result[i].VesselID < result[j].VesselID
	})
	return result
}

func (q *Queue) appendLocked(kind, vesselID string, payload any) (model.Event, error) {
	event, err := model.NewEvent(Partition, kind, vesselID, payload, q.now())
	if err != nil {
		return model.Event{}, err
	}
	return q.journal.Append(event)
}

// restore applies a journal event during replay; it never appends.
func (q *Queue) restore(event model.Event) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	switch event.Kind {
	case EventRequested:
		var payload requestedPayload
		if err := decodePayload(event, &payload); err != nil {
			return err
		}
		demand, err := newDemand(payload.VesselID, payload.Urgency, payload.Liters, payload.RequestedAt)
		if err != nil {
			return err
		}
		demand.Sequence = event.Sequence
		q.demands[payload.VesselID] = demand
	case EventDispatched:
		var payload dispatchPayload
		if err := decodePayload(event, &payload); err != nil {
			return err
		}
		demand := q.demands[payload.VesselID]
		next, err := demand.claimed(payload.SessionID, payload.DispatchedAt)
		if err != nil {
			return err
		}
		q.demands[payload.VesselID] = next
	case EventFulfilled:
		var payload resolvePayload
		if err := decodePayload(event, &payload); err != nil {
			return err
		}
		demand := q.demands[payload.VesselID]
		next, err := demand.fulfilled(payload.At)
		if err != nil {
			return err
		}
		q.demands[payload.VesselID] = next
	case EventCancelled:
		var payload cancelPayload
		if err := decodePayload(event, &payload); err != nil {
			return err
		}
		demand := q.demands[payload.VesselID]
		next, err := demand.cancelled(payload.Reason, payload.At)
		if err != nil {
			return err
		}
		q.demands[payload.VesselID] = next
	case EventRequeued:
		var payload requeuePayload
		if err := decodePayload(event, &payload); err != nil {
			return err
		}
		demand := q.demands[payload.VesselID]
		next, err := demand.requeued(payload.At)
		if err != nil {
			return err
		}
		q.demands[payload.VesselID] = next
	}
	return nil
}

type requestedPayload struct {
	VesselID    string    `json:"vessel_id"`
	Urgency     Urgency   `json:"urgency"`
	Liters      float64   `json:"liters"`
	RequestedAt time.Time `json:"requested_at"`
}

type dispatchPayload struct {
	VesselID     string    `json:"vessel_id"`
	SessionID    string    `json:"session_id"`
	DispatchedAt time.Time `json:"dispatched_at"`
}

type resolvePayload struct {
	VesselID  string    `json:"vessel_id"`
	SessionID string    `json:"session_id"`
	At        time.Time `json:"at"`
}

type cancelPayload struct {
	VesselID  string    `json:"vessel_id"`
	SessionID string    `json:"session_id,omitempty"`
	Reason    string    `json:"reason"`
	At        time.Time `json:"at"`
}

type requeuePayload struct {
	VesselID  string    `json:"vessel_id"`
	SessionID string    `json:"session_id"`
	At        time.Time `json:"at"`
}
