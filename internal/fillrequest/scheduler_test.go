package fillrequest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeLine models one shared fill circuit: it is either free or occupied.
type fakeLine struct {
	mu       sync.Mutex
	free     bool
	circuit  string
	failNext bool
	starts   []string
	aborts   []string
	counter  int
}

func newFakeLine() *fakeLine { return &fakeLine{free: true, circuit: "C-A"} }

func (f *fakeLine) AvailableFor(string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.free {
		return "", false
	}
	return f.circuit, true
}

type lineStarter struct{ line *fakeLine }

func (s *lineStarter) StartAutomaticOn(_ context.Context, vesselID string, _ float64, circuitID string) (string, error) {
	s.line.mu.Lock()
	defer s.line.mu.Unlock()
	if !s.line.free {
		return "", errors.New("circuit is already reserved")
	}
	if s.line.failNext {
		s.line.failNext = false
		return "", errors.New("valve open rejected")
	}
	s.line.free = false
	s.line.counter++
	id := fmt.Sprintf("sess-%d", s.line.counter)
	s.line.starts = append(s.line.starts, vesselID+":"+id)
	return id, nil
}

func (s *lineStarter) AbortSession(_ context.Context, sessionID, _ string) error {
	s.line.mu.Lock()
	defer s.line.mu.Unlock()
	s.line.aborts = append(s.line.aborts, sessionID)
	s.line.free = true
	return nil
}

func (f *fakeLine) release() {
	f.mu.Lock()
	f.free = true
	f.mu.Unlock()
}

func newTestScheduler(t *testing.T, clock *controllableClock) (*Scheduler, *Queue, *fakeLine) {
	t.Helper()
	queue := newTestQueue(t, newTestStore(t), clock)
	line := newFakeLine()
	scheduler, err := NewScheduler(queue, &lineStarter{line: line}, line, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	return scheduler, queue, line
}

func TestDispatchWaitsForFreeCircuit(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	scheduler, queue, line := newTestScheduler(t, clock)

	if _, err := scheduler.Submit("V-1", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	// Circuit occupied: the demand must survive rather than vanish.
	line.free = false
	results := scheduler.Dispatch(context.Background())
	if len(results) != 1 || results[0].Started || results[0].Skipped == "" {
		t.Fatalf("busy result=%+v", results)
	}
	if queued := queue.Queued(); len(queued) != 1 {
		t.Fatalf("demand lost while circuit busy: %d queued", len(queued))
	}

	// Circuit frees: next pass starts the fill.
	line.release()
	results = scheduler.Dispatch(context.Background())
	if len(results) != 1 || !results[0].Started {
		t.Fatalf("free result=%+v", results)
	}
	d, _ := queue.Get("V-1")
	if d.State != StateDispatched || d.SessionID != "sess-1" {
		t.Fatalf("dispatched=%+v", d)
	}
}

func TestDispatchStartFailureRequeuesDemand(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	scheduler, queue, line := newTestScheduler(t, clock)

	if _, err := scheduler.Submit("V-1", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	line.failNext = true
	if results := scheduler.Dispatch(context.Background()); results[0].Started {
		t.Fatal("failed start reported as started")
	}
	if queued := queue.Queued(); len(queued) != 1 {
		t.Fatalf("demand not returned after start failure: %d", len(queued))
	}
	// Retry succeeds.
	if results := scheduler.Dispatch(context.Background()); !results[0].Started {
		t.Fatalf("retry did not start: %+v", results)
	}
}

func TestDispatchHonorsPriorityAndAging(t *testing.T) {
	base := time.Unix(7000, 0)
	clock := &controllableClock{t: base}
	scheduler, _, line := newTestScheduler(t, clock)

	// Normal demand arrives first; a fresh critical demand jumps ahead.
	if _, err := scheduler.Submit("V-NORMAL", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Minute)
	if _, err := scheduler.Submit("V-CRIT", UrgencyCritical, 1); err != nil {
		t.Fatal(err)
	}
	results := scheduler.Dispatch(context.Background())
	started := startedVessels(results)
	if len(started) != 1 || started[0] != "V-CRIT" {
		t.Fatalf("critical must start first, got %v", started)
	}

	// Finish the critical fill; normal starts next.
	line.release()
	results = scheduler.Dispatch(context.Background())
	if started = startedVessels(results); len(started) != 1 || started[0] != "V-NORMAL" {
		t.Fatalf("normal should start once the line frees, got %v", started)
	}

	// Aging scenario: a long-waiting normal demand beats a fresh critical.
	line.release()
	clock.t = base.Add(10 * time.Minute)
	s2, _, _ := newTestScheduler(t, clock)
	if _, err := s2.Submit("V-OLD", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	clock.advance(5 * time.Minute)
	if _, err := s2.Submit("V-NEWCRIT", UrgencyCritical, 1); err != nil {
		t.Fatal(err)
	}
	results = s2.Dispatch(context.Background())
	if started = startedVessels(results); len(started) != 1 || started[0] != "V-OLD" {
		t.Fatalf("aged normal demand starved, got %v", started)
	}
}

func TestDispatchNeverOpensDuplicateRouteForVessel(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	scheduler, queue, line := newTestScheduler(t, clock)

	if _, err := scheduler.Submit("V-1", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	// Repeated low level probes while waiting are deduplicated.
	if err := scheduler.SubmitAutomatic("V-1", 1); err != nil {
		t.Fatalf("duplicate automatic demand errored: %v", err)
	}
	scheduler.Dispatch(context.Background())
	scheduler.Dispatch(context.Background())
	scheduler.Dispatch(context.Background())
	if len(line.starts) != 1 {
		t.Fatalf("valve route opened %d times for one vessel: %v", len(line.starts), line.starts)
	}
	d, _ := queue.Get("V-1")
	if d.State != StateDispatched {
		t.Fatalf("state=%s", d.State)
	}
	// Even a fresh explicit submit while dispatched is deduplicated.
	if _, err := scheduler.Submit("V-1", UrgencyCritical, 1); !errors.Is(err, ErrActiveDemand) {
		t.Fatalf("submit while dispatched err=%v", err)
	}
}

func TestSessionFinalizerResolvesDemand(t *testing.T) {
	clock := &controllableClock{t: time.Unix(1000, 0)}
	scheduler, queue, line := newTestScheduler(t, clock)

	if _, err := scheduler.Submit("V-1", UrgencyNormal, 1); err != nil {
		t.Fatal(err)
	}
	results := scheduler.Dispatch(context.Background())
	sessionID := results[0].SessionID
	if err := scheduler.OnSessionFinal(sessionID, true, ""); err != nil {
		t.Fatal(err)
	}
	d, _ := queue.Get("V-1")
	if d.State != StateFulfilled {
		t.Fatalf("completed session left demand %s", d.State)
	}
	// The circuit is released when a session completes in production.
	line.release()
	// A new demand is allowed after fulfilment.
	if _, err := scheduler.Submit("V-1", UrgencyNormal, 1); err != nil {
		t.Fatalf("re-request after fulfil: %v", err)
	}

	// Failed session releases the demand as cancelled.
	results = scheduler.Dispatch(context.Background())
	if err := scheduler.OnSessionFinal(results[0].SessionID, false, "route leak"); err != nil {
		t.Fatal(err)
	}
	d, _ = queue.Get("V-1")
	if d.State != StateCancelled || d.CancelReason != "route leak" {
		t.Fatalf("failed session demand=%+v", d)
	}
}

func startedVessels(results []DispatchResult) []string {
	out := make([]string, 0)
	for _, r := range results {
		if r.Started {
			out = append(out, r.Demand.VesselID)
		}
	}
	return out
}
