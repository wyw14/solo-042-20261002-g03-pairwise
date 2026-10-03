package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

// Level urgency thresholds in millimetres, matched against the low-level
// probe reading.
const (
	criticalLevelMM = 5.0
	highLevelMM     = 12.0
)

type Service struct {
	coordinator   *Coordinator
	completion    *CompletionService
	abort         *AbortService
	queue         *DemandQueue
	defaultLiters float64
}

func NewService(coordinator *Coordinator, completion *CompletionService, abort *AbortService, queue *DemandQueue, defaultLiters float64) (*Service, error) {
	if defaultLiters <= 0 {
		return nil, errors.New("default fill volume must be positive")
	}
	if queue == nil {
		return nil, errors.New("fill demand queue is required")
	}
	return &Service{
		coordinator:   coordinator,
		completion:    completion,
		abort:         abort,
		queue:         queue,
		defaultLiters: defaultLiters,
	}, nil
}

// urgencyForLevel maps a level reading to an urgency band.
func urgencyForLevel(levelMM float64) model.FillDemandUrgency {
	switch {
	case levelMM > 0 && levelMM <= criticalLevelMM:
		return model.UrgencyCritical
	case levelMM <= highLevelMM:
		return model.UrgencyHigh
	default:
		return model.UrgencyNormal
	}
}

// OnLowLevel collects a low-level probe observation into the demand queue
// instead of opening a fill immediately. While every transfer circuit is busy
// the demand simply waits; repeated observations for the same vessel are
// deduplicated onto the existing demand.
func (s *Service) OnLowLevel(ctx context.Context, vesselID string, levelMM float64, observedAt time.Time) (model.FillDemand, error) {
	_ = ctx
	_ = observedAt
	demand, _, err := s.queue.Request(vesselID, urgencyForLevel(levelMM), s.defaultLiters, model.FillAutomatic, levelMM)
	return demand, err
}

// Dispatch runs one queue scheduling pass, starting sessions only when a
// compatible circuit is free.
func (s *Service) Dispatch(ctx context.Context) []DispatchResult {
	return s.queue.Dispatch(ctx)
}

func (s *Service) Demands() *DemandQueue {
	return s.queue
}

// StartManual starts an operator-requested fill immediately. If an automatic
// demand for the vessel is waiting, it is bound to the new session so the
// demand stays tracked and is fulfilled by this fill.
func (s *Service) StartManual(ctx context.Context, vesselID string, liters float64) (model.FillSession, error) {
	if liters <= 0 {
		liters = s.defaultLiters
	}
	session, err := s.coordinator.StartManual(ctx, vesselID, liters)
	if err != nil {
		return model.FillSession{}, err
	}
	_, attached, attachErr := s.queue.Attach(vesselID, session)
	if attachErr != nil {
		// The session is open but its demand could not be bound (a journal
		// failure). Shut it back down so no untracked fill keeps running.
		_, _ = s.abort.Abort(ctx, session.ID, "demand bind failed: "+attachErr.Error())
		return model.FillSession{}, attachErr
	}
	if attached {
		current, ok := s.coordinator.Get(session.ID)
		if !ok || !current.Active() {
			// The session ended in the small window between reservation and
			// binding, so its release callback missed the demand. Replay it.
			s.queue.ReplayRelease(session.ID, vesselID, current.State == model.FillCompleted)
			return model.FillSession{}, errors.New("manual fill ended before its demand could be bound")
		}
	}
	return session, nil
}

func (s *Service) BeginSoak(id uuid.UUID) (model.FillSession, error) {
	return s.coordinator.BeginSoak(id)
}

func (s *Service) Complete(ctx context.Context, id uuid.UUID) (model.FillSession, error) {
	return s.completion.Complete(ctx, id)
}

func (s *Service) Abort(ctx context.Context, id uuid.UUID, reason string) (model.FillSession, error) {
	return s.abort.Abort(ctx, id, reason)
}

func (s *Service) RecoverIsolation(id uuid.UUID) error {
	return s.abort.RecoverIsolation(id)
}

func (s *Service) Get(id uuid.UUID) (model.FillSession, bool) {
	return s.coordinator.Get(id)
}

func (s *Service) List() []model.FillSession {
	return s.coordinator.List()
}
