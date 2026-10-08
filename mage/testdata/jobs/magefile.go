//go:build mage
// +build mage

package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/magefile/mage/mg"
)

// Max runs a dependency chain and a fan-out, then prints the most targets that
// did work at the same time.
func Max() {
	mg.Deps(chainA, mg.F(leaf, "a"), mg.F(leaf, "b"), mg.F(leaf, "c"), mg.F(leaf, "d"))
	work()
	fmt.Printf("max: %d\n", atomic.LoadInt32(&most))
}

func chainA() { mg.Deps(chainB); work() }
func chainB() { mg.Deps(chainC); work() }
func chainC() { work() }

func leaf(string) { work() }

var running, most int32

func work() {
	n := atomic.AddInt32(&running, 1)
	for {
		m := atomic.LoadInt32(&most)
		if n <= m || atomic.CompareAndSwapInt32(&most, m, n) {
			break
		}
	}
	time.Sleep(20 * time.Millisecond)
	atomic.AddInt32(&running, -1)
}
