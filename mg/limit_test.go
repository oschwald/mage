package mg

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDepsLimitChainDeeperThanLimit(t *testing.T) {
	useLimiter(t, 2)
	tr := &tracker{}
	z := func() { tr.work() }
	y := func() { Deps(z); tr.work() }
	x := func() { Deps(y); tr.work() }
	w := func() { Deps(x); tr.work() }

	finishes(t, func() { Deps(w) })
	if tr.max > 2 {
		t.Fatalf("expected at most 2 targets at once, got %d", tr.max)
	}
}

func TestDepsLimitOneRunsSerially(t *testing.T) {
	useLimiter(t, 1)
	tr := &tracker{}
	leaf := func(string) { tr.work() }

	finishes(t, func() { Deps(F(leaf, "a"), F(leaf, "b"), F(leaf, "c"), F(leaf, "d")) })
	if tr.max != 1 {
		t.Fatalf("expected 1 target at a time, got %d", tr.max)
	}
}

func TestDepsLimitSharedDependency(t *testing.T) {
	useLimiter(t, 2)
	tr := &tracker{}
	var shared int32
	c := func() { atomic.AddInt32(&shared, 1); tr.work() }
	a := func() { Deps(c); tr.work() }
	b := func() { Deps(c); tr.work() }
	leaf := func(string) { Deps(c); tr.work() }

	finishes(t, func() { Deps(a, b, F(leaf, "x"), F(leaf, "y"), F(leaf, "z")) })
	if shared != 1 {
		t.Fatalf("expected shared dependency to run once, ran %d times", shared)
	}
	if tr.max > 2 {
		t.Fatalf("expected at most 2 targets at once, got %d", tr.max)
	}
}

func TestDepsLimitFailureReturnsSlots(t *testing.T) {
	l := useLimiter(t, 2)
	fails := func() error { return errors.New("boom") }
	panics := func() { panic("boom") }
	parent := func() { Deps(fails, panics) }

	finishes(t, func() {
		defer func() {
			if recover() == nil {
				t.Error("expected Deps to panic")
			}
		}()
		Deps(parent)
	})
	if l.active != 1 {
		t.Fatalf("expected only the top-level slot in use, got %d", l.active)
	}
}

func TestDepsLimitDepsFromUserGoroutines(t *testing.T) {
	useLimiter(t, 1)
	leaf := func(string) { time.Sleep(10 * time.Millisecond) }
	parent := func() {
		var wg sync.WaitGroup
		for _, name := range []string{"a", "b", "c"} {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				Deps(F(leaf, name))
			}(name)
		}
		wg.Wait()
	}

	finishes(t, func() { Deps(parent) })
}

func TestDepsNoLimitByDefault(t *testing.T) {
	// Ignore MAGEFILE_JOBS from the shell, and any limiter an earlier test
	// made from it.
	t.Setenv(JobsEnv, "")
	ResetDeps()
	t.Cleanup(ResetDeps)
	if depLimiter() != nil {
		t.Fatalf("expected no limiter when %s is not set", JobsEnv)
	}
	// Each dependency waits for all the others to start, so this deadlocks
	// under any limit below 4.
	var wg sync.WaitGroup
	wg.Add(4)
	meet := func(string) { wg.Done(); wg.Wait() }

	finishes(t, func() { Deps(F(meet, "a"), F(meet, "b"), F(meet, "c"), F(meet, "d")) })
}

func TestDepsLimitAllowsMaxAtOnce(t *testing.T) {
	useLimiter(t, 2)
	tr := &tracker{}
	leaf := func(string) { tr.work() }

	finishes(t, func() { Deps(F(leaf, "a"), F(leaf, "b"), F(leaf, "c"), F(leaf, "d")) })
	if tr.max != 2 {
		t.Fatalf("expected 2 targets at once, got %d", tr.max)
	}
}

func TestDepsLimitCancelStopsQueuedDeps(t *testing.T) {
	useLimiter(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var started int32
	leaf := func(string) {
		atomic.AddInt32(&started, 1)
		time.Sleep(50 * time.Millisecond)
	}
	time.AfterFunc(10*time.Millisecond, cancel)

	finishes(t, func() {
		defer func() {
			if recover() == nil {
				t.Error("expected CtxDeps to panic with the cancel error")
			}
		}()
		CtxDeps(ctx, F(leaf, "a"), F(leaf, "b"), F(leaf, "c"), F(leaf, "d"))
	})
	if started != 1 {
		t.Fatalf("expected 1 dependency to start before the cancel, got %d", started)
	}
}

func TestDepsLimitKeepsSlotWhenDepsAlreadyRan(t *testing.T) {
	l := useLimiter(t, 1)
	a := func() {}
	b := func() {}
	Deps(a, b)

	// A queued target waits for the only slot, which the test goroutine holds.
	queued := make(chan struct{})
	go func() {
		_ = l.acquire(context.Background())
		close(queued)
	}()
	time.Sleep(10 * time.Millisecond)

	finishes(t, func() {
		Deps(a, b)
		SerialDeps(a, b)
	})
	select {
	case <-queued:
		t.Fatal("expected Deps to keep its slot because its dependencies already ran")
	default:
	}
	l.release()
	<-queued
	l.release()
}

func TestResetDepsReadsJobsAgain(t *testing.T) {
	useLimiter(t, 2)
	if got := depLimiter().max; got != 2 {
		t.Fatalf("expected a limit of 2, got %d", got)
	}
	t.Setenv(JobsEnv, "3")
	ResetDeps()
	if got := depLimiter().max; got != 3 {
		t.Fatalf("expected a limit of 3 after ResetDeps, got %d", got)
	}
}

func TestJobs(t *testing.T) {
	tests := map[string]int{"": 0, "abc": 0, "-1": 0, "0": 0, "3": 3}
	for val, want := range tests {
		t.Setenv(JobsEnv, val)
		if got := Jobs(); got != want {
			t.Errorf("%s=%q: expected %d, got %d", JobsEnv, val, want, got)
		}
	}
}

// useLimiter sets MAGEFILE_JOBS to maxSlots and returns the limiter that Deps
// uses. The test goroutine acts as the top-level target, so it holds the first
// slot.
func useLimiter(t *testing.T, maxSlots int) *limiter {
	t.Helper()
	t.Setenv(JobsEnv, strconv.Itoa(maxSlots))
	ResetDeps()
	t.Cleanup(ResetDeps)
	return depLimiter()
}

// tracker records the maximum number of targets that do work at once.
type tracker struct {
	running, max int32
}

func (tr *tracker) work() {
	n := atomic.AddInt32(&tr.running, 1)
	for {
		m := atomic.LoadInt32(&tr.max)
		if n <= m || atomic.CompareAndSwapInt32(&tr.max, m, n) {
			break
		}
	}
	time.Sleep(10 * time.Millisecond)
	atomic.AddInt32(&tr.running, -1)
}

// finishes fails the test if f does not return within 5 seconds.
func finishes(t *testing.T, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: Deps did not return within 5 seconds")
	}
}
