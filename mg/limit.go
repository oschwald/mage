package mg

import "sync"

// limiter caps how many targets do work at the same time. A target holds a
// slot while it runs, but gives the slot back while it waits in Deps. Thus a
// chain of dependencies deeper than the limit cannot deadlock.
type limiter struct {
	mu     sync.Mutex
	max    int
	active int
	// freed is closed and replaced each time a slot is freed, to wake the
	// goroutines that wait in acquire.
	freed chan struct{}
}

var (
	depLimiterOnce sync.Once
	depLimiterVal  *limiter
)

// depLimiter returns the limiter for MAGEFILE_JOBS, or nil if there is no
// limit.
func depLimiter() *limiter {
	depLimiterOnce.Do(func() {
		if n := Jobs(); n > 0 {
			depLimiterVal = newLimiter(n)
		}
	})
	return depLimiterVal
}

// newLimiter returns a limiter with maxSlots slots. The top-level target that
// the mage binary runs holds the first slot.
func newLimiter(maxSlots int) *limiter {
	return &limiter{max: maxSlots, active: 1, freed: make(chan struct{})}
}

// resetDepLimiter makes the next depLimiter call read MAGEFILE_JOBS again.
func resetDepLimiter() {
	depLimiterOnce = sync.Once{}
	depLimiterVal = nil
}

// acquire blocks until a slot is free, then takes it and returns true. It
// returns false if done is closed before a slot is free. A nil done never
// closes. A nil limiter does nothing and returns true.
func (l *limiter) acquire(done <-chan struct{}) bool {
	if l == nil {
		return true
	}
	for {
		l.mu.Lock()
		if l.active < l.max {
			l.active++
			l.mu.Unlock()
			return true
		}
		freed := l.freed
		l.mu.Unlock()
		select {
		case <-freed:
		case <-done:
			return false
		}
	}
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
	close(l.freed)
	l.freed = make(chan struct{})
	l.mu.Unlock()
}
