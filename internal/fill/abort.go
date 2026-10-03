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
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, errors.New("fill session not found")
	}
	if !session.Active() {
		s.coordinator.mu.Unlock()
		return session, nil
	}
	if err := s.coordinator.routes.Close(ctx, session.ID); err != nil {
		_ = s.coordinator.circuits.RetainFault(session.CircuitID, session.ID)
		failed := session.Failed(reason+": "+err.Error(), s.now())
		s.coordinator.sessions[session.ID] = failed
		s.coordinator.mu.Unlock()
		return failed, err
	}
	if err := s.coordinator.circuits.Release(session.CircuitID, session.ID); err != nil {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, err
	}
	failed := session.Failed(reason, s.now())
	if _, err := s.coordinator.vessels.FinishFill(session.VesselID); err != nil {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, err
	}
	s.coordinator.finishState(failed)
	s.coordinator.mu.Unlock()
	s.coordinator.notifyReleased(failed, false, reason)
	return failed, nil
}

func (s *AbortService) RecoverIsolation(sessionID uuid.UUID) error {
	s.coordinator.mu.Lock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		s.coordinator.mu.Unlock()
		return errors.New("fill session not found")
	}
	vesselID := session.VesselID
	if err := s.coordinator.circuits.Recover(session.CircuitID, session.ID); err != nil {
		s.coordinator.mu.Unlock()
		return err
	}
	delete(s.coordinator.active, vesselID)
	if _, err := s.coordinator.vessels.FinishFill(vesselID); err != nil {
		s.coordinator.mu.Unlock()
		return err
	}
	s.coordinator.mu.Unlock()
	s.coordinator.notifyReleased(session, false, "isolation recovered")
	return nil
}
