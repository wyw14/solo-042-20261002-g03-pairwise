package fillrequest

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/journal"
)

type controllableClock struct{ t time.Time }

func (c *controllableClock) now() time.Time          { return c.t }
func (c *controllableClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestStore(t *testing.T) *journal.Store {
	t.Helper()
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func newTestQueue(t *testing.T, store *journal.Store, clock *controllableClock) *Queue {
	t.Helper()
	queue, err := NewQueue(store, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

func TestRequestDeduplicatesActiveDemand(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	queue := newTestQueue(t, newTestStore(t), clock)

	first, err := queue.Request("V-1", UrgencyHigh, 3)
	if err != nil {
		t.Fatal(err)
	}
	second, err := queue.Request("V-1", UrgencyCritical, 9)
	if !errors.Is(err, ErrActiveDemand) {
		t.Fatalf("expected ErrActiveDemand, got %v", err)
	}
	if second.VesselID != "V-1" || second.Sequence != first.Sequence {
		t.Fatal("duplicate request should return the existing demand unchanged")
	}
	// Original urgency/volume must not be overwritten by the duplicate.
	if second.Urgency != UrgencyHigh || second.Liters != 3 {
		t.Fatalf("duplicate mutated demand: %+v", second)
	}
	if len(queue.Queued()) != 1 {
		t.Fatalf("queued=%d want 1", len(queue.Queued()))
	}
}

func TestRequestAllowedAgainAfterTerminal(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	queue := newTestQueue(t, newTestStore(t), clock)

	if _, err := queue.Request("V-1", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Cancel("V-1", "operator retracted"); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Minute)
	again, err := queue.Request("V-1", UrgencyNormal, 1)
	if err != nil {
		t.Fatalf("new request after cancel should succeed: %v", err)
	}
	if again.State != StateQueued {
		t.Fatalf("state=%s", again.State)
	}
}

func TestLeaseBindResolveRoundTrip(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	store := newTestStore(t)
	queue := newTestQueue(t, store, clock)

	if _, err := queue.Request("V-1", UrgencyHigh, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Lease("V-1"); err != nil {
		t.Fatal(err)
	}
	// A leased demand is no longer queued and cannot be re-leased.
	if len(queue.Queued()) != 0 {
		t.Fatal("leased demand still reported queued")
	}
	if _, err := queue.Lease("V-1"); !errors.Is(err, ErrActiveDemand) {
		t.Fatalf("re-lease err=%v want ErrActiveDemand", err)
	}
	bound, err := queue.Bind("V-1", "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if bound.SessionID != "sess-1" || bound.State != StateDispatched {
		t.Fatalf("bound=%+v", bound)
	}
	resolved, tracked, err := queue.ResolveBySession("sess-1", OutcomeFulfilled, "")
	if err != nil || !tracked {
		t.Fatalf("resolve tracked=%v err=%v", tracked, err)
	}
	if resolved.State != StateFulfilled {
		t.Fatalf("state=%s", resolved.State)
	}
	// Manual sessions without a demand resolve as not tracked.
	if _, tracked, err := queue.ResolveBySession("sess-manual", OutcomeFulfilled, ""); err != nil || tracked {
		t.Fatalf("manual session tracked=%v err=%v", tracked, err)
	}
}

func TestLeaseReleaseKeepsDemandQueued(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	queue := newTestQueue(t, newTestStore(t), clock)

	if _, err := queue.Request("V-1", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Lease("V-1"); err != nil {
		t.Fatal(err)
	}
	if err := queue.ReleaseLease("V-1"); err != nil {
		t.Fatal(err)
	}
	if queued := queue.Queued(); len(queued) != 1 || queued[0].VesselID != "V-1" {
		t.Fatalf("demand not returned to queue: %+v", queued)
	}
}

func TestCancelRejectsDispatchedDemand(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	queue := newTestQueue(t, newTestStore(t), clock)

	if _, err := queue.Request("V-1", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Lease("V-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Bind("V-1", "sess-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Cancel("V-1", "x"); err == nil {
		t.Fatal("dispatched demand must not be cancellable directly")
	}
	if _, err := queue.Cancel("V-1", ""); err == nil {
		t.Fatal("blank cancel reason accepted")
	}
}

func TestRecoveryRebuildsStateFromJournal(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	store := newTestStore(t)
	queue := newTestQueue(t, store, clock)

	// V-1 requested then dispatched (still in flight at crash).
	if _, err := queue.Request("V-1", UrgencyHigh, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Lease("V-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Bind("V-1", "sess-1"); err != nil {
		t.Fatal(err)
	}
	// V-2 requested and cancelled.
	if _, err := queue.Request("V-2", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Cancel("V-2", "not needed"); err != nil {
		t.Fatal(err)
	}
	// V-3 still queued.
	clock.advance(time.Minute)
	if _, err := queue.Request("V-3", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}

	reopened := newTestQueue(t, store, clock)
	if err := NewRecovery(store, reopened).Restore(); err != nil {
		t.Fatal(err)
	}

	d1, ok := reopened.Get("V-1")
	if !ok || d1.State != StateDispatched || d1.SessionID != "sess-1" {
		t.Fatalf("V-1 recovered=%+v ok=%v", d1, ok)
	}
	d2, ok := reopened.Get("V-2")
	if !ok || d2.State != StateCancelled {
		t.Fatalf("V-2 recovered=%+v", d2)
	}
	d3, ok := reopened.Get("V-3")
	if !ok || d3.State != StateQueued {
		t.Fatalf("V-3 recovered=%+v", d3)
	}

	// No live sessions after restart: the dispatched demand is requeued,
	// preserving its original request time.
	count, err := reopened.RequeueDispatched(map[string]bool{})
	if err != nil || count != 1 {
		t.Fatalf("requeue count=%d err=%v", count, err)
	}
	d1, _ = reopened.Get("V-1")
	if d1.State != StateQueued || d1.SessionID != "" {
		t.Fatalf("V-1 not requeued: %+v", d1)
	}
	if !d1.RequestedAt.Equal(time.Unix(1000, 0)) {
		t.Fatalf("requeue lost original request time: %v", d1.RequestedAt)
	}
}

func TestRequeueSkipsLiveSessions(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	queue := newTestQueue(t, newTestStore(t), clock)

	if _, err := queue.Request("V-1", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Lease("V-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Bind("V-1", "sess-1"); err != nil {
		t.Fatal(err)
	}
	// A session the coordinator still tracks after restart is untouched.
	count, err := queue.RequeueDispatched(map[string]bool{"sess-1": true})
	if err != nil || count != 0 {
		t.Fatalf("live session requeued count=%d err=%v", count, err)
	}
	d1, _ := queue.Get("V-1")
	if d1.State != StateDispatched {
		t.Fatalf("live session demand changed: %+v", d1)
	}
}

func TestQueuedOrderReflectsPriority(t *testing.T) {
	base := time.Unix(5000, 0)
	clock := &controllableClock{t: base}
	queue := newTestQueue(t, newTestStore(t), clock)

	if _, err := queue.Request("V-N", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Minute)
	if _, err := queue.Request("V-C", UrgencyCritical, 1); err != nil {
		t.Fatal(err)
	}
	queued := queue.Queued()
	if queued[0].VesselID != "V-C" {
		t.Fatalf("critical should lead initially, got %s", queued[0].VesselID)
	}

	// Advance past two aging steps: normal demand is promoted and is
	// older, so it must lead.
	clock.advance(4 * time.Minute)
	queued = queue.Queued()
	if queued[0].VesselID != "V-N" {
		t.Fatalf("aged normal demand starved: order=%v", vesselIDs(queued))
	}
}

func vesselIDs(demands []Demand) []string {
	ids := make([]string, len(demands))
	for i, d := range demands {
		ids[i] = d.VesselID
	}
	return ids
}
