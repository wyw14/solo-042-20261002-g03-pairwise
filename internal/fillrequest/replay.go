package fillrequest

import (
	"encoding/json"

	"github.com/wyw14/cry-98/internal/model"
)

func decodePayload(event model.Event, target any) error {
	return json.Unmarshal(event.Payload, target)
}

// Recovery replays the fill-requests journal partition into a queue.
type Recovery struct {
	store EventsReader
	queue *Queue
}

// EventsReader is the read side of the journal. *journal.Store
// satisfies it via Events.
type EventsReader interface {
	Events(partition string, after uint64) ([]model.Event, error)
}

func NewRecovery(store EventsReader, queue *Queue) *Recovery {
	return &Recovery{store: store, queue: queue}
}

// Restore replays every demand lifecycle event in journal order.
func (r *Recovery) Restore() error {
	events, err := r.store.Events(Partition, 0)
	if err != nil {
		return err
	}
	for _, event := range events {
		if event.Partition != "" && event.Partition != Partition {
			continue
		}
		if err := r.queue.restore(event); err != nil {
			return err
		}
	}
	return nil
}
