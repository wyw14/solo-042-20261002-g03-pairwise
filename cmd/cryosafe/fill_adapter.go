package main

import (
	"context"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/fill"
	"github.com/wyw14/cry-98/internal/model"
)

// fillStarterAdapter adapts the fill coordinator to the demand
// scheduler's FillStarter interface without making fill depend on
// fillrequest.
type fillStarterAdapter struct {
	coordinator *fill.Coordinator
	aborts      *fill.AbortService
}

func (a *fillStarterAdapter) StartAutomaticOn(ctx context.Context, vesselID string, liters float64, circuitID string) (string, error) {
	session, err := a.coordinator.StartOnCircuit(ctx, vesselID, model.FillAutomatic, liters, circuitID)
	if err != nil {
		return "", err
	}
	return session.ID.String(), nil
}

func (a *fillStarterAdapter) AbortSession(ctx context.Context, sessionID, reason string) error {
	id, err := uuid.Parse(sessionID)
	if err != nil {
		return err
	}
	_, err = a.aborts.Abort(ctx, id, reason)
	return err
}
