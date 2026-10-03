package fillrequest

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// SubmitAutomatic records an automatic refill demand from a low level
// probe. It satisfies fill.AutomaticDemandSink. A duplicate demand while
// one is already live is deduplicated silently — that is the expected
// steady state for a probe that keeps reporting low level.
func (s *Scheduler) SubmitAutomatic(vesselID string, liters float64) error {
	_, err := s.queue.Request(vesselID, UrgencyNormal, liters)
	if errors.Is(err, ErrActiveDemand) {
		return nil
	}
	return err
}

// Submit records a demand with explicit urgency, e.g. for operators or
// higher-level policies. It returns the recorded or existing demand.
func (s *Scheduler) Submit(vesselID string, urgency Urgency, liters float64) (Demand, error) {
	return s.queue.Request(vesselID, urgency, liters)
}

// Cancel retracts a queued demand for a vessel.
func (s *Scheduler) Cancel(vesselID, reason string) (Demand, error) {
	return s.queue.Cancel(vesselID, reason)
}

// List returns every demand (including terminal ones) for tracking.
func (s *Scheduler) List() []Demand {
	return s.queue.List()
}

// Queued returns demands awaiting dispatch in scheduling order.
func (s *Scheduler) Queued() []Demand {
	return s.queue.Queued()
}

// Get returns the latest demand for a vessel.
func (s *Scheduler) Get(vesselID string) (Demand, bool) {
	return s.queue.Get(vesselID)
}

// FillStarter creates an automatic fill session for a vessel pinned to
// a chosen circuit, and can abort a session if dispatch bookkeeping
// fails. The fill package adapter satisfies it, returning the new
// session id.
type FillStarter interface {
	StartAutomaticOn(ctx context.Context, vesselID string, liters float64, circuitID string) (sessionID string, err error)
	AbortSession(ctx context.Context, sessionID, reason string) error
}

// CircuitMatcher reports whether a compatible, currently free fill
// circuit exists for a vessel. Compatibility covers physical routing
// constraints such as the vessel's zone.
type CircuitMatcher interface {
	AvailableFor(vesselID string) (circuitID string, available bool)
}

// DispatchResult describes what the scheduler did with one queued
// demand during a Dispatch pass.
type DispatchResult struct {
	Demand    Demand `json:"demand"`
	Started   bool   `json:"started"`
	SessionID string `json:"session_id,omitempty"`
	Skipped   string `json:"skipped_reason,omitempty"`
}

// Scheduler claims queued demands and creates fill sessions only when a
// compatible circuit is available. It never starts a session directly:
// probes submit demands and the runtime drives Dispatch periodically.
type Scheduler struct {
	queue    *Queue
	starter  FillStarter
	circuits CircuitMatcher
	now      func() time.Time
	log      *slog.Logger
}

func NewScheduler(queue *Queue, starter FillStarter, circuits CircuitMatcher, now func() time.Time) (*Scheduler, error) {
	if queue == nil || starter == nil || circuits == nil {
		return nil, errors.New("scheduler dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &Scheduler{
		queue:    queue,
		starter:  starter,
		circuits: circuits,
		now:      now,
		log:      slog.Default(),
	}, nil
}

// SetLogger overrides the default structured logger.
func (s *Scheduler) SetLogger(log *slog.Logger) {
	if log != nil {
		s.log = log
	}
}

// Dispatch performs one scheduling pass over the queued demands in
// priority order. A demand is claimed only when a compatible circuit is
// free; demands that cannot start yet stay queued and are retried on a
// later pass, so a busy circuit never drops a request.
func (s *Scheduler) Dispatch(ctx context.Context) []DispatchResult {
	results := make([]DispatchResult, 0)
	for _, demand := range s.queue.Queued() {
		if err := ctx.Err(); err != nil {
			s.log.Warn("fill demand dispatch interrupted", "vessel", demand.VesselID, "error", err)
			return results
		}
		result := DispatchResult{Demand: demand}
		circuitID, available := s.circuits.AvailableFor(demand.VesselID)
		if !available {
			// No compatible free circuit right now. Leave the demand
			// queued; do not block other vessels from being considered.
			result.Skipped = "no compatible circuit available"
			results = append(results, result)
			continue
		}
		// Reserve the demand before touching any valve so two passes can
		// never open a route for the same vessel concurrently.
		if _, err := s.queue.Lease(demand.VesselID); err != nil {
			result.Skipped = "lease failed: " + err.Error()
			results = append(results, result)
			continue
		}
		sessionID, err := s.starter.StartAutomaticOn(ctx, demand.VesselID, demand.Liters, circuitID)
		if err != nil {
			_ = s.queue.ReleaseLease(demand.VesselID)
			// The coordinator rejects duplicate active sessions and busy
			// circuits; either way the demand stays queued for a retry.
			s.log.Warn("fill demand could not start",
				"vessel", demand.VesselID, "circuit", circuitID, "error", err)
			result.Skipped = err.Error()
			results = append(results, result)
			continue
		}
		if _, err := s.queue.Bind(demand.VesselID, sessionID); err != nil {
			// The session exists but dispatch could not be journaled.
			// Tear the session down so no fill runs untracked, then
			// return the demand to the queue for the next pass.
			s.log.Error("dispatch journal failed; aborting started session",
				"vessel", demand.VesselID, "session", sessionID, "error", err)
			if abortErr := s.starter.AbortSession(ctx, sessionID, "dispatch tracking failed"); abortErr != nil {
				s.log.Error("could not abort untracked session; demand held until recovery",
					"vessel", demand.VesselID, "session", sessionID, "error", abortErr)
			} else {
				_ = s.queue.ReleaseLease(demand.VesselID)
			}
			result.Skipped = "dispatch tracking failed"
			results = append(results, result)
			continue
		}
		result.Started = true
		result.SessionID = sessionID
		results = append(results, result)
		s.log.Info("fill demand dispatched",
			"vessel", demand.VesselID, "session", sessionID,
			"circuit", circuitID, "urgency", demand.Urgency)
	}
	return results
}

// OnSessionFinal is invoked when a fill session reaches a terminal
// state. Completed sessions fulfil their demand; failed/aborted
// sessions release it so the vessel can request again. Sessions without
// a matching demand (manual fills) are ignored.
func (s *Scheduler) OnSessionFinal(sessionID string, completed bool, failure string) error {
	outcome := OutcomeCancelled
	reason := failure
	if completed {
		outcome = OutcomeFulfilled
		reason = ""
	} else if reason == "" {
		reason = "fill session ended unsuccessfully"
	}
	demand, tracked, err := s.queue.ResolveBySession(sessionID, outcome, reason)
	if err != nil {
		s.log.Error("failed to resolve demand for session",
			"session", sessionID, "error", err)
		return err
	}
	if tracked {
		s.log.Info("fill demand resolved",
			"vessel", demand.VesselID, "session", sessionID, "outcome", outcome)
	}
	return nil
}
