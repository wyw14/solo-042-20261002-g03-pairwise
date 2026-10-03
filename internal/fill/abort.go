package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

type AbortService struct {
	coordinator *Coordinator
	now         func() time.Time
}

func NewAbortService(coordinator *Coordinator, now func() time.Time) *AbortService {
	if now == nil {
		now = time.Now
	}
	return &AbortService{coordinator: coordinator, now: now}
}

func (s *AbortService) Abort(ctx context.Context, sessionID uuid.UUID, reason string) (model.FillSession, error) {
	if reason == "" {
		return model.FillSession{}, errors.New("abort reason is required")
	}
	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		return model.FillSession{}, errors.New("fill session not found")
	}
	if !session.Active() {
		return session, nil
	}
	if err := s.coordinator.routes.Close(ctx, session.ID); err != nil {
		_ = s.coordinator.circuits.RetainFault(session.CircuitID, session.ID)
		failed := session.Failed(reason+": "+err.Error(), s.now())
		s.coordinator.sessions[session.ID] = failed
		return failed, err
	}
	if err := s.coordinator.circuits.Release(session.CircuitID, session.ID); err != nil {
		return model.FillSession{}, err
	}
	failed := session.Failed(reason, s.now())
	if _, err := s.coordinator.vessels.FinishFill(session.VesselID); err != nil {
		return model.FillSession{}, err
	}
	s.coordinator.finishState(failed)
	return failed, nil
}

func (s *AbortService) RecoverIsolation(sessionID uuid.UUID) error {
	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		return errors.New("fill session not found")
	}
	if err := s.coordinator.circuits.Recover(session.CircuitID, session.ID); err != nil {
		return err
	}
	// finishState removes the active reservation and resolves any
	// linked demand; a faulted session never fulfilled its request.
	s.coordinator.finishState(session)
	_, err := s.coordinator.vessels.FinishFill(session.VesselID)
	return err
}
