package fillrequest

import (
	"testing"
	"time"
)

func TestPriorityAgingPreventsStarvation(t *testing.T) {
	base := time.Unix(1_000_000, 0)

	oldNormal, err := newDemand("V-OLD", UrgencyNormal, 1.0, base)
	if err != nil {
		t.Fatal(err)
	}
	freshCritical, err := newDemand("V-NEW", UrgencyCritical, 1.0, base.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	// Fresh arrivals: critical outranks normal.
	if got := oldNormal.Priority(base); got != UrgencyNormal {
		t.Fatalf("fresh normal priority=%d want %d", got, UrgencyNormal)
	}
	if got := freshCritical.Priority(base.Add(5 * time.Minute)); got != UrgencyCritical {
		t.Fatalf("critical priority=%d want %d", got, UrgencyCritical)
	}

	// After 5 minutes (>= 2 aging steps) the normal demand is promoted
	// to critical and, being older, must be scheduled before the fresh
	// critical demand — proving it cannot starve.
	at := base.Add(5 * time.Minute)
	if got := oldNormal.Priority(at); got != UrgencyCritical {
		t.Fatalf("aged normal priority=%d want critical", got)
	}
	if !Order(oldNormal, freshCritical, at) {
		t.Fatal("aged normal demand starved behind a fresh critical demand")
	}
	if Order(freshCritical, oldNormal, at) {
		t.Fatal("ordering must be asymmetric")
	}
}

func TestOrderUrgencyThenWaitThenSequence(t *testing.T) {
	base := time.Unix(2_000_000, 0)
	normal, _ := newDemand("V-N", UrgencyNormal, 1, base)
	high, _ := newDemand("V-H", UrgencyHigh, 1, base.Add(time.Minute))
	normal.Sequence = 1
	high.Sequence = 2

	if !Order(high, normal, base.Add(time.Minute)) {
		t.Fatal("higher urgency should come first at the same time")
	}

	// Two equal-urgency demands: the older one wins even with a higher
	// submission sequence.
	older, _ := newDemand("V-A", UrgencyNormal, 1, base)
	newer, _ := newDemand("V-B", UrgencyNormal, 1, base.Add(30*time.Second))
	newer.Sequence, older.Sequence = 1, 2
	if !Order(older, newer, base.Add(time.Minute)) {
		t.Fatal("older demand should win among equal priorities")
	}

	// Same urgency and time: lower sequence wins.
	a, _ := newDemand("V-A", UrgencyNormal, 1, base)
	b, _ := newDemand("V-B", UrgencyNormal, 1, base)
	a.Sequence, b.Sequence = 3, 4
	if !Order(a, b, base) {
		t.Fatal("lower sequence should break the tie")
	}
}

func TestDemandLifecycleTransitions(t *testing.T) {
	base := time.Unix(3_000_000, 0)
	d, err := newDemand("V-1", UrgencyHigh, 2.5, base)
	if err != nil {
		t.Fatal(err)
	}
	if d.State != StateQueued {
		t.Fatalf("new demand state=%s", d.State)
	}
	leased, err := d.leased(base)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := leased.bound("session-1", base)
	if err != nil {
		t.Fatal(err)
	}
	if bound.SessionID != "session-1" {
		t.Fatalf("bound session=%q", bound.SessionID)
	}
	fulfilled, err := bound.fulfilled(base.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !fulfilled.State.terminal() {
		t.Fatal("fulfilled demand must be terminal")
	}
	if _, err := fulfilled.cancelled("x", base); err == nil {
		t.Fatal("terminal demand must not be cancellable")
	}

	// Requeue preserves the original request time.
	again, _ := newDemand("V-2", UrgencyNormal, 1, base)
	again, _ = again.leased(base)
	again, _ = again.bound("session-2", base)
	requeued, err := again.requeued(base.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if requeued.State != StateQueued || requeued.SessionID != "" {
		t.Fatalf("requeued demand not reset: %+v", requeued)
	}
	if !requeued.RequestedAt.Equal(base) {
		t.Fatal("requeue must preserve original request time")
	}
}

func TestNewDemandValidates(t *testing.T) {
	base := time.Unix(4_000_000, 0)
	if _, err := newDemand(" ", UrgencyNormal, 1, base); err == nil {
		t.Fatal("blank vessel accepted")
	}
	if _, err := newDemand("V-1", Urgency(99), 1, base); err == nil {
		t.Fatal("invalid urgency accepted")
	}
	if _, err := newDemand("V-1", UrgencyNormal, 0, base); err == nil {
		t.Fatal("non-positive liters accepted")
	}
}
