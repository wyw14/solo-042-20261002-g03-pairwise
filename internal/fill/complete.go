package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/pressure"
)

type CompletionService struct {
	coordinator *Coordinator
	pressure    *pressure.Monitor
	journal     EventAppender
	now         func() time.Time
}

type EventAppender interface {
	Append(model.Event) (model.Event, error)
}

func NewCompletionService(coordinator *Coordinator, pressureMonitor *pressure.Monitor, store EventAppender, now func() time.Time) *CompletionService {
	if now == nil {
		now = time.Now
	}
	return &CompletionService{coordinator: coordinator, pressure: pressureMonitor, journal: store, now: now}
}

func (s *CompletionService) Complete(ctx context.Context, sessionID uuid.UUID) (model.FillSession, error) {
	if err := ctx.Err(); err != nil {
		return model.FillSession{}, err
	}
	s.coordinator.mu.Lock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, errors.New("fill session not found")
	}
	if session.State != model.FillSoaking {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, errors.New("fill session is not soaking")
	}
	result, err := s.pressure.Evaluate(session.VesselID)
	if err != nil {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, err
	}
	if !result.Stable {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, errors.New("pressure soak window is not stable")
	}
	event, err := model.NewEvent("fills", "fill.soak_verified", session.ID.String(), result, s.now())
	if err != nil {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, err
	}
	if _, err := s.journal.Append(event); err != nil {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, err
	}
	verified, err := session.Transition(model.FillVerified, s.now())
	if err != nil {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, err
	}
	completed, err := verified.Transition(model.FillCompleted, s.now())
	if err != nil {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, err
	}
	if err := s.coordinator.routes.Close(ctx, session.ID); err != nil {
		_ = s.coordinator.circuits.RetainFault(session.CircuitID, session.ID)
		s.coordinator.mu.Unlock()
		return model.FillSession{}, err
	}
	if err := s.coordinator.circuits.Release(session.CircuitID, session.ID); err != nil {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, err
	}
	if _, err := s.coordinator.vessels.FinishFill(session.VesselID); err != nil {
		s.coordinator.mu.Unlock()
		return model.FillSession{}, err
	}
	s.coordinator.finishState(completed)
	s.coordinator.mu.Unlock()
	s.coordinator.notifyReleased(completed, true, "")
	return completed, nil
}
