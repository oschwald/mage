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

// newLimiter returns a limiter with max slots, of which held are already in
// use.
func newLimiter(maxSlots, held int) *limiter {
	l := &limiter{max: maxSlots, active: held}
	l.cond = sync.NewCond(&l.mu)
	return l
}

var (
	depLimiterOnce sync.Once
	depLimiterVal  *limiter
)

// depLimiter returns the limiter for MAGEFILE_JOBS, or nil if there is no
// limit. The top-level target that the mage binary runs holds the first slot.
func depLimiter() *limiter {
	depLimiterOnce.Do(func() {
		if n := Jobs(); n > 0 {
			depLimiterVal = newLimiter(n, 1)
		}
	})
	return depLimiterVal
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
// the real number of running targets. More targets than max can then run for
// a short time, but this cannot cause a deadlock.
func (l *limiter) release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.active--
	l.mu.Unlock()
	l.cond.Signal()
}
