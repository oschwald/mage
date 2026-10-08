package mg

import "sync"

// limiter caps how many targets do work at the same time. A target holds a
// slot while it runs, but gives the slot back while it waits in Deps. Thus a
// chain of dependencies deeper than the limit cannot deadlock.
type limiter struct {
	mu     sync.Mutex
	cond   *sync.Cond
	max    int
	active int
}

var (
	depLimiterMu  sync.Mutex
	depLimiterSet bool
	depLimiterVal *limiter
)

// depLimiter returns the limiter for MAGEFILE_JOBS, or nil if there is no
// limit.
func depLimiter() *limiter {
	depLimiterMu.Lock()
	defer depLimiterMu.Unlock()
	if !depLimiterSet {
		depLimiterSet = true
		if n := Jobs(); n > 0 {
			depLimiterVal = newLimiter(n)
		}
	}
	return depLimiterVal
}

// newLimiter returns a limiter with maxSlots slots. The top-level target that
// the mage binary runs holds the first slot.
func newLimiter(maxSlots int) *limiter {
	l := &limiter{max: maxSlots, active: 1}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// resetDepLimiter makes the next depLimiter call read MAGEFILE_JOBS again.
func resetDepLimiter() {
	depLimiterMu.Lock()
	defer depLimiterMu.Unlock()
	depLimiterSet = false
	depLimiterVal = nil
}

// acquire blocks until a slot is free, then takes it. A nil limiter does
// nothing.
func (l *limiter) acquire() {
	if l == nil {
		return
	}
	l.mu.Lock()
	for l.active >= l.max {
		l.cond.Wait()
	}
	l.active++
	l.mu.Unlock()
}

// release gives back a slot. A nil limiter does nothing.
//
// If Deps is called from a goroutine that holds no slot, active can go below
// the real number of running targets. Then more than max targets can run
// until that Deps call returns, but this cannot cause a deadlock.
func (l *limiter) release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.active--
	l.mu.Unlock()
	l.cond.Signal()
}
