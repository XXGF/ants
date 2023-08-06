// MIT License

// Copyright (c) 2018 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ants

import (
	"errors"
	"log"
	"math"
	"os"
	"runtime"
	"time"
)

const (
	// DefaultAntsPoolSize is the default capacity for a default goroutine pool.
	DefaultAntsPoolSize = math.MaxInt32

	// DefaultCleanIntervalTime is the interval time to clean up goroutines.
	DefaultCleanIntervalTime = time.Second
)

const (
	// OPENED represents that the pool is opened.
	OPENED = iota

	// CLOSED represents that the pool is closed.
	CLOSED
)

var (
	// ErrLackPoolFunc will be returned when invokers don't provide function for pool.
	ErrLackPoolFunc = errors.New("must provide function for pool")

	// ErrInvalidPoolExpiry will be returned when setting a negative number as the periodic duration to purge goroutines.
	ErrInvalidPoolExpiry = errors.New("invalid expiry for pool")

	// ErrPoolClosed will be returned when submitting task to a closed pool.
	ErrPoolClosed = errors.New("this pool has been closed")

	// ErrPoolOverload will be returned when the pool is full and no workers available.
	ErrPoolOverload = errors.New("too many goroutines blocked on submit or Nonblocking is set")

	// ErrInvalidPreAllocSize will be returned when trying to set up a negative capacity under PreAlloc mode.
	ErrInvalidPreAllocSize = errors.New("can not set up a negative capacity under PreAlloc mode")

	// ErrTimeout will be returned after the operations timed out.
	ErrTimeout = errors.New("operation timed out")

	// workerChanCap determines whether the channel of a worker should be a buffered channel
	// to get the best performance. Inspired by fasthttp at
	// https://github.com/valyala/fasthttp/blob/master/workerpool.go#L139
	workerChanCap = func() int {
		// Use blocking channel if GOMAXPROCS=1.
		// This switches context from sender to receiver immediately,
		// which results in higher performance (under go1.5 at least).
		// 如果CPU是单核，则使用阻塞channel，即容量为0的channel
		if runtime.GOMAXPROCS(0) == 1 {
			return 0
		}

		// 注意：
		// runtime.GOMAXPROCS的入参n，是一个整数类型的参数，表示要设置的最大CPU数。
		// - 如果n的值小于1，则不会改变GOMAXPROCS的值。
		// - 如果n的值大于1，则会将GOMAXPROCS的值设置为n，并返回之前的GOMAXPROCS值。

		// Use non-blocking workerChan if GOMAXPROCS>1,
		// since otherwise the sender might be dragged down if the receiver is CPU-bound.
		// 否则，使用容量为 1 的channel，虽然不阻塞，但是也只能容纳一个task
		return 1
	}()

	// log.Lmsgprefix is not available in go1.13, just make an identical value for it.
	logLmsgprefix = 64
	defaultLogger = Logger(log.New(os.Stderr, "[ants]: ", log.LstdFlags|logLmsgprefix|log.Lmicroseconds))

	// Init an instance pool when importing ants.
	defaultAntsPool, _ = NewPool(DefaultAntsPoolSize)
)

const nowTimeUpdateInterval = 500 * time.Millisecond

// Logger is used for logging formatted messages.
type Logger interface {
	// Printf must have the same semantics as log.Printf.
	Printf(format string, args ...interface{})
}

// Submit submits a task to pool.
func Submit(task func()) error {
	return defaultAntsPool.Submit(task)
}

// Running returns the number of the currently running goroutines.
func Running() int {
	return defaultAntsPool.Running()
}

// Cap returns the capacity of this default pool.
func Cap() int {
	return defaultAntsPool.Cap()
}

// Free returns the available goroutines to work.
func Free() int {
	return defaultAntsPool.Free()
}

// Release Closes the default pool.
func Release() {
	defaultAntsPool.Release()
}

// ReleaseTimeout is like Release but with a timeout, it waits all workers to exit before timing out.
func ReleaseTimeout(timeout time.Duration) error {
	return defaultAntsPool.ReleaseTimeout(timeout)
}

// Reboot reboots the default pool.
func Reboot() {
	defaultAntsPool.Reboot()
}
