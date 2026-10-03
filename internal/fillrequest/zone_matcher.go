package fillrequest

import (
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/vessel"
)

// ZoneCircuitMatcher finds an enabled, unreserved circuit in the
// vessel's zone. Circuits serve physically routed zones, so a north
// vessel must never be filled through a south circuit even when that
// circuit is the only free one.
type ZoneCircuitMatcher struct {
	vessels  *vessel.Registry
	circuits *circuit.Registry
	reserved *circuit.Manager
}

// NewZoneCircuitMatcher wires the matcher to the live registries.
func NewZoneCircuitMatcher(vessels *vessel.Registry, circuits *circuit.Registry, reservations *circuit.Manager) *ZoneCircuitMatcher {
	return &ZoneCircuitMatcher{vessels: vessels, circuits: circuits, reserved: reservations}
}

// AvailableFor returns the first enabled, free circuit in the vessel's
// zone, deterministically ordered by circuit id.
func (m *ZoneCircuitMatcher) AvailableFor(vesselID string) (string, bool) {
	target, ok := m.vessels.Get(vesselID)
	if !ok {
		return "", false
	}
	for _, candidate := range m.circuits.List() {
		if !candidate.Enabled || candidate.Zone != target.Zone {
			continue
		}
		if _, occupied := m.reserved.Get(candidate.ID); occupied {
			continue
		}
		return candidate.ID, true
	}
	return "", false
}
