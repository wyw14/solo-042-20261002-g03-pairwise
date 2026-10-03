package fill

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
)

const demandEventPartition = "fill-demands"

var (
	ErrDemandNotFound = errors.New("fill demand not found")
)

// DemandEventAppender persists demand lifecycle events.
type DemandEventAppender interface {
	Append(model.Event) (model.Event, error)
}

// SchedulingPolicy controls urgency aging and failure backoff.
type SchedulingPolicy struct {
	// BoostEvery: a queued demand gains one urgency band per BoostEvery of
	// waiting time, so an old low-priority demand is guaranteed to overtake
	// fresh higher-priority ones and can never starve.
	BoostEvery time.Duration
	// FailureBackoff is the base delay before re-attempting a demand whose
	// start failed for a reason other than a busy circuit. It grows linearly
	// with the attempt count and is capped at FailureBackoffCap.
	FailureBackoff    time.Duration
	FailureBackoffCap time.Duration
}

func DefaultSchedulingPolicy() SchedulingPolicy {
	return SchedulingPolicy{
		BoostEvery:        2 * time.Minute,
		FailureBackoff:    5 * time.Second,
		FailureBackoffCap: time.Minute,
	}
}

func (p SchedulingPolicy) valid() bool {
	return p.BoostEvery > 0 && p.FailureBackoff > 0 && p.FailureBackoffCap >= p.FailureBackoff
}

func (p SchedulingPolicy) backoff(attempts int) time.Duration {
	delay := p.FailureBackoff * time.Duration(attempts)
	if delay > p.FailureBackoffCap || delay < 0 {
		return p.FailureBackoffCap
	}
	return delay
}

// DispatchResult reports what happened for one demand during a dispatch pass.
type DispatchResult struct {
	Demand  model.FillDemand
	Session model.FillSession
	// Started is true when a new fill session was created.
	Started bool
	// Busy is true when the demand stayed queued because no circuit (or the
	// vessel's own session slot) was available.
	Busy bool
	Err  error
}

// demandEvent is the journal payload for every demand lifecycle event.
type demandEvent struct {
	Demand model.FillDemand `json:"demand"`
	Reason string           `json:"reason,omitempty"`
}

type DemandQueue struct {
	mu           sync.Mutex
	dispatchMu   sync.Mutex
	demands      map[uuid.UUID]model.FillDemand
	openByVessel map[string]uuid.UUID
	coordinator  *Coordinator
	aborts       *AbortService
	journal      DemandEventAppender
	policy       SchedulingPolicy
	now          func() time.Time
	wake         chan struct{}
}

func NewDemandQueue(coordinator *Coordinator, journal DemandEventAppender, policy SchedulingPolicy, now func() time.Time) (*DemandQueue, error) {
	if coordinator == nil || journal == nil {
		return nil, errors.New("demand queue dependencies are required")
	}
	if !policy.valid() {
		return nil, errors.New("demand scheduling policy is invalid")
	}
	if now == nil {
		now = time.Now
	}
	queue := &DemandQueue{
		demands:      make(map[uuid.UUID]model.FillDemand),
		openByVessel: make(map[string]uuid.UUID),
		coordinator:  coordinator,
		journal:      journal,
		policy:       policy,
		now:          now,
		wake:         make(chan struct{}, 1),
	}
	coordinator.SetReleaseHook(queue.onSessionReleased)
	return queue, nil
}

func (q *DemandQueue) SetAbortService(aborts *AbortService) {
	q.mu.Lock()
	q.aborts = aborts
	q.mu.Unlock()
}

// Request collects a refill demand for a vessel. Repeated demands while one is
// already open are deduplicated: the existing demand is refreshed (urgency may
// be raised) instead of creating a second entry, and it keeps its place in the
// wait queue.
func (q *DemandQueue) Request(vesselID string, urgency model.FillDemandUrgency, liters float64, source model.FillOrigin, levelMM float64) (model.FillDemand, bool, error) {
	now := q.now()
	demand, err := model.NewFillRequest(vesselID, urgency, liters, source, levelMM, now)
	if err != nil {
		return model.FillDemand{}, false, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if openID, exists := q.openByVessel[vesselID]; exists {
		existing := q.demands[openID]
		if existing.Waiting() {
			refreshed := existing.NoteSignal(urgency, levelMM, now)
			q.demands[existing.ID] = refreshed
			if err := q.appendEventLocked("fill_demand.signaled", refreshed, ""); err != nil {
				return model.FillDemand{}, false, err
			}
			return refreshed, true, nil
		}
		// A dispatched demand already owns the vessel's valve route.
		return existing, true, nil
	}
	if err := q.appendEventLocked("fill_demand.requested", demand, ""); err != nil {
		return model.FillDemand{}, false, err
	}
	q.demands[demand.ID] = demand
	q.openByVessel[vesselID] = demand.ID
	q.signalLocked()
	return demand, false, nil
}

// Cancel closes a queued demand. A dispatched demand cannot be cancelled here:
// its valve route is already open and must be terminated through the fill
// session abort flow, which then releases the demand.
func (q *DemandQueue) Cancel(id uuid.UUID, reason string) (model.FillDemand, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	demand, ok := q.demands[id]
	if !ok {
		return model.FillDemand{}, ErrDemandNotFound
	}
	if !demand.Open() {
		return demand, nil
	}
	if !demand.Waiting() {
		return model.FillDemand{}, errors.New("demand is already dispatched; abort its fill session instead")
	}
	cancelled, err := demand.Cancel(reason, q.now())
	if err != nil {
		return model.FillDemand{}, err
	}
	if err := q.appendEventLocked("fill_demand.cancelled", cancelled, reason); err != nil {
		return model.FillDemand{}, err
	}
	q.storeClosedLocked(cancelled)
	return cancelled, nil
}

// Dispatch runs one scheduling pass: waiting demands are ordered by urgency
// with wait-time aging, and each is started only when a circuit is actually
// available. Demands that cannot be served stay queued untouched, so a busy
// transfer loop never makes a demand disappear.
func (q *DemandQueue) Dispatch(ctx context.Context) []DispatchResult {
	q.dispatchMu.Lock()
	defer q.dispatchMu.Unlock()
	planned := q.planDispatch()
	results := make([]DispatchResult, 0, len(planned))
	for _, demand := range planned {
		if err := ctx.Err(); err != nil {
			results = append(results, DispatchResult{Demand: demand, Err: err})
			continue
		}
		session, err := q.coordinator.StartForDemand(ctx, demand.VesselID, demand.Source, demand.PlannedLiters)
		if err != nil {
			busy := errors.Is(err, ErrActiveSession) || errors.Is(err, circuit.ErrCircuitBusy)
			if !busy {
				// The vessel slot was free but the start itself failed
				// (valve fault, vessel not ready...). Back the demand off so
				// it does not hammer the hardware every scheduling tick.
				q.recordFailure(demand.ID)
			}
			results = append(results, DispatchResult{Demand: demand, Busy: busy, Err: err})
			continue
		}
		claimed, demandStillOpen, claimErr := q.claim(demand.ID, session)
		if claimErr != nil {
			// The valve route is open but the dispatch could not be
			// recorded; abort the session rather than leave it untracked.
			_ = q.abortNewSession(ctx, session, "demand dispatch recording failed")
			results = append(results, DispatchResult{Demand: demand, Session: session, Err: claimErr})
			continue
		}
		if !demandStillOpen {
			// The demand was cancelled while the session was being opened.
			// Shut the just-opened valve route back down immediately rather
			// than leave an untracked fill running.
			results = append(results, DispatchResult{Demand: claimed, Session: session, Err: q.abortNewSession(ctx, session, "demand cancelled while dispatching")})
			continue
		}
		if current, ok := q.coordinator.Get(session.ID); !ok || !current.Active() {
			// The session ended (abort/completion) in the window between the
			// circuit being reserved and the demand being claimed, so its
			// release callback ran before the binding existed. Replay the
			// release now that the session id is linked; an aborted fill
			// requeues the demand, a completed one fulfils it.
			q.onSessionReleased(session.ID, demand.VesselID, current.State == model.FillCompleted, "session ended during dispatch")
			results = append(results, DispatchResult{Demand: claimed, Session: session, Err: errors.New("fill session ended before the demand could be bound")})
			continue
		}
		results = append(results, DispatchResult{Demand: claimed, Session: session, Started: true})
	}
	return results
}

// planDispatch snapshots the waiting demands that currently have a usable
// circuit, in scheduling order.
func (q *DemandQueue) planDispatch() []model.FillDemand {
	q.mu.Lock()
	now := q.now()
	waiting := make([]model.FillDemand, 0, len(q.demands))
	for _, demand := range q.demands {
		if demand.Waiting() && !now.Before(demand.NotBefore) {
			waiting = append(waiting, demand)
		}
	}
	q.mu.Unlock()
	sort.Slice(waiting, func(i, j int) bool {
		return q.scheduledBefore(waiting[i], waiting[j])
	})
	planned := make([]model.FillDemand, 0, len(waiting))
	reserved := make(map[string]bool)
	for _, demand := range waiting {
		if q.coordinator.HasActiveSession(demand.VesselID) {
			continue
		}
		circuit, available := q.coordinator.AvailableCircuit()
		if !available || reserved[circuit.ID] {
			continue
		}
		reserved[circuit.ID] = true
		planned = append(planned, demand)
	}
	return planned
}

// scheduledBefore orders demands for dispatch. The primary key is urgency
// raised by wait time; ties break on the original arrival time, so equal
// urgency is strict FIFO and aging guarantees bounded waits for low priority.
func (q *DemandQueue) scheduledBefore(a, b model.FillDemand) bool {
	scoreA := a.Urgency.Rank() + int(q.now().Sub(a.ReceivedAt)/q.policy.BoostEvery)
	scoreB := b.Urgency.Rank() + int(q.now().Sub(b.ReceivedAt)/q.policy.BoostEvery)
	if scoreA != scoreB {
		return scoreA > scoreB
	}
	if !a.ReceivedAt.Equal(b.ReceivedAt) {
		return a.ReceivedAt.Before(b.ReceivedAt)
	}
	return a.ID.String() < b.ID.String()
}

// claim records the dispatch of a demand after its session was created. It
// re-reads the demand under the lock to close the cancel/dispatch race. The
// returned bool reports whether the demand was still open (queued).
func (q *DemandQueue) claim(demandID uuid.UUID, session model.FillSession) (model.FillDemand, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	demand, ok := q.demands[demandID]
	if !ok {
		return model.FillDemand{}, false, ErrDemandNotFound
	}
	if !demand.Open() {
		return demand, false, nil
	}
	claimed, err := demand.Claim(session.ID, q.now())
	if err != nil {
		return model.FillDemand{}, true, err
	}
	if err := q.appendEventLocked("fill_demand.dispatched", claimed, ""); err != nil {
		return model.FillDemand{}, true, err
	}
	q.demands[demandID] = claimed
	return claimed, true, nil
}

// recordFailure applies an in-memory backoff to a demand whose dispatch start
// failed, so later scheduling passes skip it until the backoff elapses.
func (q *DemandQueue) recordFailure(demandID uuid.UUID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	demand, ok := q.demands[demandID]
	if !ok || !demand.Waiting() {
		return
	}
	demand.Attempts++
	demand.NotBefore = q.now().Add(q.policy.backoff(demand.Attempts)).UTC()
	q.demands[demandID] = demand
}

func (q *DemandQueue) abortNewSession(ctx context.Context, session model.FillSession, reason string) error {
	q.mu.Lock()
	aborts := q.aborts
	q.mu.Unlock()
	if aborts == nil {
		return errors.New("cannot abort session opened during dispatch: abort service unavailable")
	}
	_, err := aborts.Abort(ctx, session.ID, reason)
	return err
}

// ReplayRelease re-applies a session release against the demand bound to a
// vessel. It handles the race where a session ends in the window between
// reservation and the demand being claimed, before the binding existed.
func (q *DemandQueue) ReplayRelease(sessionID uuid.UUID, vesselID string, succeeded bool) {
	q.onSessionReleased(sessionID, vesselID, succeeded, "release replayed after dispatch race")
}

// onSessionReleased is invoked by the coordinator (outside its lock) whenever
// a fill session ends: a completed session fulfils its demand, an aborted one
// requeues it so the vessel is not left unfilled, and the freed circuit is a
// chance to schedule the next waiting demand.
func (q *DemandQueue) onSessionReleased(sessionID uuid.UUID, vesselID string, succeeded bool, reason string) {
	q.mu.Lock()
	openID, hasOpen := q.openByVessel[vesselID]
	if !hasOpen {
		q.mu.Unlock()
		q.signal()
		return
	}
	demand := q.demands[openID]
	if demand.SessionID != sessionID || !demand.Open() {
		q.mu.Unlock()
		q.signal()
		return
	}
	now := q.now()
	if succeeded {
		if closed, err := demand.Fulfill(now); err == nil {
			// Persist first when we can, but never strand the demand: the
			// circuit is already released, so in-memory state must follow.
			// A missed event only means a restart conservatively requeues
			// the demand, which is safe.
			_ = q.appendEventLocked("fill_demand.fulfilled", closed, "")
			q.storeClosedLocked(closed)
		}
	} else {
		if requeued, err := demand.Requeue(reason, now); err == nil {
			_ = q.appendEventLocked("fill_demand.requeued", requeued, reason)
			q.demands[openID] = requeued
		}
	}
	q.mu.Unlock()
	q.signal()
}

// Attach claims an existing queued demand for a session started out-of-band
// (for example a manual operator start). It returns false when no open queued
// demand exists for the vessel.
func (q *DemandQueue) Attach(vesselID string, session model.FillSession) (model.FillDemand, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	openID, ok := q.openByVessel[vesselID]
	if !ok {
		return model.FillDemand{}, false, nil
	}
	demand := q.demands[openID]
	if !demand.Waiting() {
		return demand, false, nil
	}
	claimed, err := demand.Claim(session.ID, q.now())
	if err != nil {
		return model.FillDemand{}, false, err
	}
	if err := q.appendEventLocked("fill_demand.dispatched", claimed, "attached to out-of-band session"); err != nil {
		return model.FillDemand{}, false, err
	}
	q.demands[openID] = claimed
	return claimed, true, nil
}

func (q *DemandQueue) Get(id uuid.UUID) (model.FillDemand, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	demand, ok := q.demands[id]
	return demand, ok
}

func (q *DemandQueue) List() []model.FillDemand {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]model.FillDemand, 0, len(q.demands))
	for _, demand := range q.demands {
		result = append(result, demand)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].ReceivedAt.Equal(result[j].ReceivedAt) {
			return result[i].ReceivedAt.Before(result[j].ReceivedAt)
		}
		return result[i].ID.String() < result[j].ID.String()
	})
	return result
}

// Wake returns a channel that receives a value whenever the queue state
// changes in a way that may allow new dispatches.
func (q *DemandQueue) Wake() <-chan struct{} {
	return q.wake
}

func (q *DemandQueue) signal() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.signalLocked()
}

func (q *DemandQueue) signalLocked() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *DemandQueue) storeClosedLocked(demand model.FillDemand) {
	delete(q.openByVessel, demand.VesselID)
	q.demands[demand.ID] = demand
}

func (q *DemandQueue) appendEventLocked(kind string, demand model.FillDemand, reason string) error {
	event, err := model.NewEvent(demandEventPartition, kind, demand.ID.String(), demandEvent{Demand: demand, Reason: reason}, q.now())
	if err != nil {
		return err
	}
	_, err = q.journal.Append(event)
	return err
}
