package fill

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

// DemandRecovery rebuilds the demand queue from the "fill-demands" journal
// partition after a restart. Every lifecycle event carries the full demand
// snapshot, so replay folds to the latest snapshot per demand id.
type DemandRecovery struct {
	store *journal.Store
	queue *DemandQueue
}

func NewDemandRecovery(store *journal.Store, queue *DemandQueue) *DemandRecovery {
	return &DemandRecovery{store: store, queue: queue}
}

func (r *DemandRecovery) Restore() error {
	events, err := r.store.Events(demandEventPartition, 0)
	if err != nil {
		return err
	}
	r.queue.mu.Lock()
	defer r.queue.mu.Unlock()
	latest := make(map[uuid.UUID]model.FillDemand)
	for _, event := range events {
		var payload demandEvent
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		if payload.Demand.ID == uuid.Nil {
			continue
		}
		latest[payload.Demand.ID] = payload.Demand
	}
	for _, demand := range latest {
		// Reservations, valve routes and fill sessions live only in memory,
		// so a demand found mid-dispatch cannot trust its old session: send
		// it back to the queue to be scheduled on a fresh, known-free
		// circuit. Queued demands keep their original arrival time.
		if demand.State == model.DemandDispatched {
			requeued, err := demand.RequeueForRestart()
			if err != nil {
				return err
			}
			demand = requeued
		}
		r.queue.demands[demand.ID] = demand
		if demand.Open() {
			r.queue.openByVessel[demand.VesselID] = demand.ID
		}
	}
	if len(r.queue.openByVessel) > 0 {
		r.queue.signalLocked()
	}
	return nil
}
