package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

// AutomaticDemandSink accepts refill demands raised by low level
// probes instead of starting a fill immediately. The persisted demand
// queue (internal/fillrequest) satisfies this interface; a nil sink
// keeps the legacy direct-start behaviour.
type AutomaticDemandSink interface {
	SubmitAutomatic(vesselID string, liters float64) error
}

type Service struct {
	coordinator   *Coordinator
	completion    *CompletionService
	abort         *AbortService
	demands       AutomaticDemandSink
	defaultLiters float64
}

func NewService(coordinator *Coordinator, completion *CompletionService, abort *AbortService, defaultLiters float64) (*Service, error) {
	if defaultLiters <= 0 {
		return nil, errors.New("default fill volume must be positive")
	}
	return &Service{coordinator: coordinator, completion: completion, abort: abort, defaultLiters: defaultLiters}, nil
}

// SetDemandQueue routes low level probe events through the persisted
// demand queue. Without it, OnLowLevel falls back to starting a session
// directly.
func (s *Service) SetDemandQueue(sink AutomaticDemandSink) {
	s.demands = sink
}

// OnLowLevel handles a low level probe reading. With a demand queue
// configured it records a deduplicated demand and returns; the scheduler
// starts the fill once a compatible circuit is free, so a busy circuit
// no longer causes the request to vanish.
func (s *Service) OnLowLevel(ctx context.Context, vesselID string, observedAt time.Time) error {
	_ = observedAt
	_ = ctx
	if s.demands != nil {
		return s.demands.SubmitAutomatic(vesselID, s.defaultLiters)
	}
	_, err := s.coordinator.StartAutomatic(ctx, vesselID, s.defaultLiters)
	return err
}

func (s *Service) StartManual(ctx context.Context, vesselID string, liters float64) (model.FillSession, error) {
	if liters <= 0 {
		liters = s.defaultLiters
	}
	return s.coordinator.StartManual(ctx, vesselID, liters)
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
