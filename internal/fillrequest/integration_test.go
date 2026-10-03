package fillrequest_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/fill"
	"github.com/wyw14/cry-98/internal/fillrequest"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

type harness struct {
	coordinator *fill.Coordinator
	aborts      *fill.AbortService
	scheduler   *fillrequest.Scheduler
	manager     *circuit.Manager
	vessels     *vessel.Registry
	now         func() time.Time
	clock       *mutableClock
}

type mutableClock struct{ t time.Time }

func (c *mutableClock) now() time.Time          { return c.t }
func (c *mutableClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type coordinatorStarter struct {
	coord  *fill.Coordinator
	aborts *fill.AbortService
}

func (s *coordinatorStarter) StartAutomaticOn(ctx context.Context, vesselID string, liters float64, circuitID string) (string, error) {
	session, err := s.coord.StartOnCircuit(ctx, vesselID, model.FillAutomatic, liters, circuitID)
	if err != nil {
		return "", err
	}
	return session.ID.String(), nil
}

func (s *coordinatorStarter) AbortSession(ctx context.Context, sessionID, reason string) error {
	id, err := uuid.Parse(sessionID)
	if err != nil {
		return err
	}
	_, err = s.aborts.Abort(ctx, id, reason)
	return err
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	clock := &mutableClock{t: time.Unix(1_000_000, 0)}

	vesselRegistry := vessel.NewRegistry()
	for _, seed := range []struct{ id, name, zone string }{
		{"V-N1", "north vessel 1", "north"},
		{"V-N2", "north vessel 2", "north"},
		{"V-S1", "south vessel 1", "south"},
	} {
		v, err := model.NewVessel(seed.id, seed.name, seed.zone, clock.t)
		if err != nil {
			t.Fatal(err)
		}
		if err := vesselRegistry.Register(v); err != nil {
			t.Fatal(err)
		}
	}
	vesselState := vessel.NewStateService(vesselRegistry, clock.now)

	circuitRegistry := circuit.NewRegistry(
		circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true},
		circuit.Circuit{ID: "C-B", Zone: "south", ValveID: "VALVE-B", Enabled: true},
	)
	manager := circuit.NewManager("C-A", "C-B")

	transport := valve.NewMemoryTransport(clock.now)
	controller, err := valve.NewController(transport, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	commands := valve.NewCommandService(controller, clock.now)
	routes := valve.NewRouteService(commands)

	coordinator := fill.NewCoordinator(manager, circuitRegistry, routes, vesselState, clock.now)
	aborts := fill.NewAbortService(coordinator, clock.now)

	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	queue, err := fillrequest.NewQueue(store, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	matcher := fillrequest.NewZoneCircuitMatcher(vesselRegistry, circuitRegistry, manager)
	starter := &coordinatorStarter{coord: coordinator, aborts: aborts}
	scheduler, err := fillrequest.NewScheduler(queue, starter, matcher, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	coordinator.SetSessionFinalizer(func(session model.FillSession) {
		_ = scheduler.OnSessionFinal(session.ID.String(), session.State == model.FillCompleted, session.Failure)
	})
	return &harness{
		coordinator: coordinator,
		aborts:      aborts,
		scheduler:   scheduler,
		manager:     manager,
		vessels:     vesselRegistry,
		now:         clock.now,
		clock:       clock,
	}
}

func activeSessionCount(h *harness, vesselID string) int {
	count := 0
	for _, session := range h.coordinator.List() {
		if session.VesselID == vesselID && session.Active() {
			count++
		}
	}
	return count
}

// End-to-end: busy circuits queue demands and the scheduler never opens
// two routes for one vessel; aborting frees the circuit for the waiter.
func TestSchedulerIntegrationWithRealCoordinator(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Both circuits get occupied by one north and one south demand.
	for _, v := range []string{"V-N1", "V-S1"} {
		if _, err := h.scheduler.Submit(v, fillrequest.UrgencyNormal, 1.5); err != nil {
			t.Fatal(err)
		}
	}
	h.scheduler.Dispatch(ctx)
	if activeSessionCount(h, "V-N1") != 1 || activeSessionCount(h, "V-S1") != 1 {
		t.Fatal("first two demands should each hold a session")
	}

	// Third demand shares the north circuit, which is busy: it must wait.
	if _, err := h.scheduler.Submit("V-N2", fillrequest.UrgencyNormal, 1.5); err != nil {
		t.Fatal(err)
	}
	h.clock.advance(time.Minute)
	h.scheduler.Dispatch(ctx)
	if activeSessionCount(h, "V-N2") != 0 {
		t.Fatal("V-N2 must wait while the north circuit is reserved")
	}
	if queued := h.scheduler.Queued(); len(queued) != 1 || queued[0].VesselID != "V-N2" {
		t.Fatalf("V-N2 demand vanished: %+v", queued)
	}

	// Repeated dispatch passes while waiting must not open a second
	// north route or duplicate V-N1's session.
	h.scheduler.Dispatch(ctx)
	h.scheduler.Dispatch(ctx)
	if activeSessionCount(h, "V-N1") != 1 {
		t.Fatal("duplicate session/route opened for V-N1 while circuit busy")
	}

	// Abort the running north session: its circuit and valve route are
	// released and the demand resolves, letting V-N2 proceed.
	var northSessionID uuid.UUID
	for _, session := range h.coordinator.List() {
		if session.VesselID == "V-N1" && session.Active() {
			northSessionID = session.ID
		}
	}
	if _, err := h.aborts.Abort(ctx, northSessionID, "operator abort"); err != nil {
		t.Fatal(err)
	}
	if _, occupied := h.manager.Get("C-A"); occupied {
		t.Fatal("north circuit still reserved after abort")
	}
	if d, ok := h.scheduler.Get("V-N1"); !ok || d.State != fillrequest.StateCancelled {
		t.Fatalf("V-N1 demand not cancelled after abort: %+v", d)
	}

	h.scheduler.Dispatch(ctx)
	if activeSessionCount(h, "V-N2") != 1 {
		t.Fatal("V-N2 did not start after the north circuit freed")
	}
	if d, ok := h.scheduler.Get("V-N2"); !ok || d.State != fillrequest.StateDispatched {
		t.Fatalf("V-N2 not dispatched: %+v", d)
	}
}

// A vessel in one zone must never be routed through another zone's
// circuit, even when that is the only free circuit.
func TestSchedulerRefusesCrossZoneCircuit(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Occupy the north circuit only; south stays free.
	if _, err := h.scheduler.Submit("V-N1", fillrequest.UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	h.scheduler.Dispatch(ctx)
	// Another north vessel must wait, not spill onto the south circuit.
	if _, err := h.scheduler.Submit("V-N2", fillrequest.UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	h.scheduler.Dispatch(ctx)
	if activeSessionCount(h, "V-N2") != 0 {
		t.Fatal("V-N2 must remain unscheduled: north busy and it cannot use the south circuit")
	}
	if queued := h.scheduler.Queued(); len(queued) != 1 || queued[0].VesselID != "V-N2" {
		t.Fatalf("V-N2 demand missing or routed cross-zone: %+v", queued)
	}
}
