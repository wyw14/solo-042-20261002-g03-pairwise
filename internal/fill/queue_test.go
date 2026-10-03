package fill

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

type fakeClock struct {
	t time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type harness struct {
	t           *testing.T
	clock       *fakeClock
	registry    *vessel.Registry
	vessels     *vessel.StateService
	circuits    *circuit.Registry
	manager     *circuit.Manager
	routes      *valve.RouteService
	coordinator *Coordinator
	store       *journal.Store
	queue       *DemandQueue
	aborts      *AbortService
}

func newHarness(t *testing.T, clock *fakeClock, circuitIDs ...string) *harness {
	t.Helper()
	registry := vessel.NewRegistry()
	for _, id := range []string{"V-01", "V-02", "V-03", "V-04"} {
		item, err := model.NewVessel(id, "vessel "+id, "zone", clock.t)
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Register(item); err != nil {
			t.Fatal(err)
		}
	}
	vessels := vessel.NewStateService(registry, clock.Now)
	circuits := make([]circuit.Circuit, 0, len(circuitIDs))
	for _, id := range circuitIDs {
		circuits = append(circuits, circuit.Circuit{ID: id, Zone: "z", ValveID: "VALVE-" + id, Enabled: true})
	}
	circuitRegistry := circuit.NewRegistry(circuits...)
	manager := circuit.NewManager(circuitIDs...)
	transport := valve.NewMemoryTransport(clock.Now)
	controller, err := valve.NewController(transport, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	commands := valve.NewCommandService(controller, clock.Now)
	routes := valve.NewRouteService(commands)
	coordinator := NewCoordinator(manager, circuitRegistry, routes, vessels, clock.Now)
	retry, err := NewRetryService(commands, RetryPolicy{Attempts: 2, Delay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.SetRetryService(retry)
	store, err := journal.NewStore(t.TempDir() + "/journal")
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewDemandQueue(coordinator, store, SchedulingPolicy{
		BoostEvery:        time.Minute,
		FailureBackoff:    time.Second,
		FailureBackoffCap: time.Minute,
	}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	aborts := NewAbortService(coordinator, clock.Now)
	queue.SetAbortService(aborts)
	h := &harness{
		t:           t,
		clock:       clock,
		registry:    registry,
		vessels:     vessels,
		circuits:    circuitRegistry,
		manager:     manager,
		routes:      routes,
		coordinator: coordinator,
		store:       store,
		queue:       queue,
		aborts:      aborts,
	}
	return h
}

func mustRequest(t *testing.T, h *harness, vesselID string, urgency model.FillDemandUrgency) model.FillDemand {
	t.Helper()
	demand, dedup, err := h.queue.Request(vesselID, urgency, 2.1, model.FillAutomatic, 10)
	if err != nil {
		t.Fatalf("request for %s: %v", vesselID, err)
	}
	if dedup {
		t.Fatalf("request for %s unexpectedly deduplicated", vesselID)
	}
	return demand
}

// releaseCircuit frees a started session's circuit reservation without going
// through Complete/Abort (and therefore without touching the demand queue
// again), mirroring the post-completion hardware state for scheduling tests.
func (h *harness) releaseCircuit(session model.FillSession) {
	if err := h.manager.Recover(session.CircuitID, session.ID); err != nil {
		h.t.Fatalf("recover circuit: %v", err)
	}
	if _, err := h.vessels.FinishFill(session.VesselID); err != nil {
		h.t.Fatalf("finish vessel fill: %v", err)
	}
	h.coordinator.mu.Lock()
	delete(h.coordinator.active, session.VesselID)
	h.coordinator.mu.Unlock()
}

func TestDemandDeduplicatesAndEscalates(t *testing.T) {
	h := newHarness(t, newFakeClock(), "C-A")
	first := mustRequest(t, h, "V-01", model.UrgencyNormal)
	h.clock.advance(30 * time.Second)
	again, dedup, err := h.queue.Request("V-01", model.UrgencyCritical, 2.1, model.FillAutomatic, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !dedup || again.ID != first.ID {
		t.Fatal("repeated low-level signal created a second demand")
	}
	if again.Urgency != model.UrgencyCritical {
		t.Fatalf("urgency not escalated: %s", again.Urgency)
	}
	if !again.ReceivedAt.Equal(first.ReceivedAt) {
		t.Fatal("dedup moved the original arrival time")
	}
	if !again.LastSignalAt.Equal(h.clock.Now()) {
		t.Fatal("dedup did not refresh the last signal time")
	}
	if len(h.queue.List()) != 1 {
		t.Fatalf("queue length = %d, want 1", len(h.queue.List()))
	}
}

func TestDispatchOnlyWhenCircuitFreeAndTracksClaim(t *testing.T) {
	h := newHarness(t, newFakeClock(), "C-A")
	mustRequest(t, h, "V-01", model.UrgencyHigh)

	results := h.queue.Dispatch(context.Background())
	if len(results) != 1 || !results[0].Started {
		t.Fatalf("dispatch results = %+v", results)
	}
	if results[0].Demand.State != model.DemandDispatched || results[0].Demand.SessionID == uuid.Nil {
		t.Fatal("demand was not claimed by a session")
	}
	if results[0].Session.ID != results[0].Demand.SessionID {
		t.Fatal("claimed session id mismatch")
	}

	// A second pass has nothing to start; the demand is not double-dispatched.
	if results := h.queue.Dispatch(context.Background()); len(results) != 0 {
		t.Fatalf("second dispatch started work: %+v", results)
	}

	events, err := h.store.Events(demandEventPartition, 0)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, event := range events {
		kinds[event.Kind] = true
	}
	if !kinds["fill_demand.requested"] || !kinds["fill_demand.dispatched"] {
		t.Fatalf("missing lifecycle events: %v", kinds)
	}
}

func TestBusyCircuitKeepsDemandQueuedUntilReleased(t *testing.T) {
	h := newHarness(t, newFakeClock(), "C-A")
	// The only circuit is occupied by an unrelated fill.
	if _, err := h.coordinator.StartAutomatic(context.Background(), "V-03", 1.0); err != nil {
		t.Fatal(err)
	}
	demand := mustRequest(t, h, "V-01", model.UrgencyHigh)

	results := h.queue.Dispatch(context.Background())
	if len(results) != 0 {
		t.Fatalf("busy circuit should start nothing, got %+v", results)
	}
	stored, ok := h.queue.Get(demand.ID)
	if !ok || stored.State != model.DemandQueued {
		t.Fatalf("demand vanished while circuit busy: %+v ok=%v", stored, ok)
	}

	// Free the circuit by aborting the occupying session, then the demand
	// must be schedulable.
	sessions := h.coordinator.List()
	if _, err := h.aborts.Abort(context.Background(), sessions[0].ID, "test release"); err != nil {
		t.Fatal(err)
	}
	results = h.queue.Dispatch(context.Background())
	if len(results) != 1 || !results[0].Started || results[0].Demand.VesselID != "V-01" {
		t.Fatalf("demand not started after circuit freed: %+v", results)
	}
}

func TestDispatchOrdersByUrgency(t *testing.T) {
	h := newHarness(t, newFakeClock(), "C-A")
	mustRequest(t, h, "V-01", model.UrgencyNormal)
	h.clock.advance(5 * time.Second)
	mustRequest(t, h, "V-02", model.UrgencyCritical)
	h.clock.advance(time.Second)
	mustRequest(t, h, "V-03", model.UrgencyNormal)

	results := h.queue.Dispatch(context.Background())
	if len(results) != 1 || results[0].Demand.VesselID != "V-02" {
		t.Fatalf("critical demand must go first, got %+v", results)
	}

	// Fulfil V-02 (simulating a completed session: circuit released, demand
	// fulfilled).
	v02 := results[0]
	h.releaseCircuit(v02.Session)
	h.queue.onSessionReleased(v02.Session.ID, "V-02", true, "")

	// Advance well past the aging window: the oldest normal demand has now
	// waited much longer and must overtake the younger normal demand.
	h.clock.advance(10 * time.Minute)
	results = h.queue.Dispatch(context.Background())
	if len(results) != 1 || results[0].Demand.VesselID != "V-01" {
		t.Fatalf("aged FIFO order wrong, got %+v", results)
	}
}

func TestWaitAgingPreventsStarvation(t *testing.T) {
	h := newHarness(t, newFakeClock(), "C-A")
	mustRequest(t, h, "V-01", model.UrgencyNormal)
	// After three boost windows a normal demand outranks a fresh critical one.
	h.clock.advance(3 * time.Minute)
	mustRequest(t, h, "V-02", model.UrgencyCritical)

	results := h.queue.Dispatch(context.Background())
	if len(results) != 1 || results[0].Demand.VesselID != "V-01" {
		t.Fatalf("long-waiting low priority demand starved, got %+v", results)
	}
}

func TestCancelLifecycle(t *testing.T) {
	h := newHarness(t, newFakeClock(), "C-A")
	queued := mustRequest(t, h, "V-01", model.UrgencyNormal)
	dispatched := mustRequest(t, h, "V-02", model.UrgencyHigh)

	results := h.queue.Dispatch(context.Background())
	if len(results) != 1 || results[0].Demand.VesselID != "V-02" {
		t.Fatalf("unexpected dispatch: %+v", results)
	}

	if _, err := h.queue.Cancel(dispatched.ID, "operator"); err == nil {
		t.Fatal("dispatched demand must be cancelled through session abort")
	}

	cancelled, err := h.queue.Cancel(queued.ID, "level recovered")
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.State != model.DemandCancelled || cancelled.CancelReason != "level recovered" {
		t.Fatalf("cancel not recorded: %+v", cancelled)
	}
	// Slot released: the same vessel can request again.
	replacement := mustRequest(t, h, "V-01", model.UrgencyNormal)
	if replacement.ID == queued.ID {
		t.Fatal("cancelled vessel slot was not released")
	}

	events, err := h.store.Events(demandEventPartition, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Kind == "fill_demand.cancelled" {
			found = true
		}
	}
	if !found {
		t.Fatal("cancel was not journaled")
	}
}

func TestAbortRequeuesDemand(t *testing.T) {
	h := newHarness(t, newFakeClock(), "C-A")
	demand := mustRequest(t, h, "V-01", model.UrgencyHigh)
	first := h.queue.Dispatch(context.Background())
	if len(first) != 1 {
		t.Fatalf("dispatch = %+v", first)
	}

	if _, err := h.aborts.Abort(context.Background(), first[0].Session.ID, "operator stop"); err != nil {
		t.Fatal(err)
	}
	stored, ok := h.queue.Get(demand.ID)
	if !ok || stored.State != model.DemandQueued {
		t.Fatalf("aborted demand not requeued: %+v ok=%v", stored, ok)
	}
	if !stored.ReceivedAt.Equal(demand.ReceivedAt) {
		t.Fatal("requeue lost accumulated wait time")
	}

	again := h.queue.Dispatch(context.Background())
	if len(again) != 1 || !again[0].Started {
		t.Fatalf("requeued demand not redispatched: %+v", again)
	}
	h.queue.onSessionReleased(again[0].Session.ID, "V-01", true, "")
	fulfilled, _ := h.queue.Get(demand.ID)
	if fulfilled.State != model.DemandFulfilled {
		t.Fatalf("demand not fulfilled: %+v", fulfilled)
	}
}

func TestActiveVesselSessionIsNeverDispatchedTwice(t *testing.T) {
	h := newHarness(t, newFakeClock(), "C-A")
	demand := mustRequest(t, h, "V-01", model.UrgencyHigh)
	// A session for the same vessel is opened out-of-band.
	if _, err := h.coordinator.StartManual(context.Background(), "V-01", 1.0); err != nil {
		t.Fatal(err)
	}
	if results := h.queue.Dispatch(context.Background()); len(results) != 0 {
		t.Fatalf("dispatch must skip a vessel with an active session: %+v", results)
	}
	stored, _ := h.queue.Get(demand.ID)
	if stored.State != model.DemandQueued {
		t.Fatalf("demand state changed unexpectedly: %+v", stored)
	}
	if _, occupied := h.manager.Get("C-A"); !occupied {
		t.Fatal("the single circuit must still hold exactly one reservation")
	}
	if _, err := h.coordinator.StartManual(context.Background(), "V-01", 1.0); err != ErrActiveSession {
		t.Fatalf("second session for vessel should be rejected, got %v", err)
	}
}

func TestDemandRecoversAfterRestart(t *testing.T) {
	clock := newFakeClock()
	h := newHarness(t, clock, "C-A")
	queued := mustRequest(t, h, "V-01", model.UrgencyNormal)
	dispatched := mustRequest(t, h, "V-02", model.UrgencyHigh)
	first := h.queue.Dispatch(context.Background())
	if len(first) != 1 || first[0].Demand.VesselID != "V-02" {
		t.Fatalf("dispatch = %+v", first)
	}

	// Simulate a full process restart: fresh in-memory state over the same
	// journal store.
	restarted := newHarness(t, clock, "C-A")
	restarted.queue = mustNewQueue(t, restarted.coordinator, h.store, clock)
	if err := NewDemandRecovery(h.store, restarted.queue).Restore(); err != nil {
		t.Fatal(err)
	}

	recoveredQueued, ok := restarted.queue.Get(queued.ID)
	if !ok || recoveredQueued.State != model.DemandQueued || !recoveredQueued.ReceivedAt.Equal(queued.ReceivedAt) {
		t.Fatalf("queued demand not restored with wait time: %+v", recoveredQueued)
	}
	recoveredDispatched, ok := restarted.queue.Get(dispatched.ID)
	if !ok || recoveredDispatched.State != model.DemandQueued || recoveredDispatched.SessionID != uuid.Nil {
		t.Fatalf("dispatched demand must return to queue after restart: %+v", recoveredDispatched)
	}

	// Fresh manager has both circuits free: the requeued demand is served.
	results := restarted.queue.Dispatch(context.Background())
	if len(results) != 1 || results[0].Demand.VesselID != "V-02" {
		t.Fatalf("restart dispatch = %+v", results)
	}

	// Once V-02 completes (circuit released, demand fulfilled), V-01 goes.
	restarted.releaseCircuit(results[0].Session)
	restarted.queue.onSessionReleased(results[0].Session.ID, "V-02", true, "")
	results = restarted.queue.Dispatch(context.Background())
	if len(results) != 1 || results[0].Demand.VesselID != "V-01" {
		t.Fatalf("second restart dispatch = %+v", results)
	}
}

func TestReplayReleaseBindsOutcomeAfterDispatchRace(t *testing.T) {
	h := newHarness(t, newFakeClock(), "C-A")
	demand := mustRequest(t, h, "V-01", model.UrgencyHigh)
	session, err := h.coordinator.StartForDemand(context.Background(), "V-01", model.FillAutomatic, 2.1)
	if err != nil {
		t.Fatal(err)
	}
	// The session ends (abort) before the dispatch binding exists, so its
	// release callback misses the demand. The dispatch then claims and, seeing
	// the dead session, replays the release.
	h.releaseCircuit(session)
	if _, ok, err := h.queue.claim(demand.ID, session); err != nil || !ok {
		t.Fatalf("claim = %v %v", ok, err)
	}
	h.queue.ReplayRelease(session.ID, "V-01", false)
	stored, _ := h.queue.Get(demand.ID)
	if stored.State != model.DemandQueued || stored.SessionID != uuid.Nil {
		t.Fatalf("demand not requeued by release replay: %+v", stored)
	}
}

func mustNewQueue(t *testing.T, coordinator *Coordinator, store *journal.Store, clock *fakeClock) *DemandQueue {
	t.Helper()
	queue, err := NewDemandQueue(coordinator, store, SchedulingPolicy{
		BoostEvery:        time.Minute,
		FailureBackoff:    time.Second,
		FailureBackoffCap: time.Minute,
	}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	queue.SetAbortService(NewAbortService(coordinator, clock.Now))
	return queue
}
