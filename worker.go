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
	"runtime/debug"
	"time"
)

// goWorker is the actual executor who runs the tasks,
// it starts a goroutine that accepts tasks and
// performs function calls.
// goWorker 是真正的任务执行者。它会开启一个协程去接受任务和执行任务函数。
type goWorker struct {
	// pool who owns this worker.
	// goWorker所属协程驰
	pool *Pool

	// task is a job should be done.
	// 接收任务的channel，这个channel是用于 提交任务协程 和 执行任务协程 之间的通信。
	task chan func()

	// lastUsed will be updated when putting a worker back into queue.
	// 当它worker放回队列的时候，这个时间会被更新
	lastUsed time.Time
}

// run starts a goroutine to repeat the process
// that performs the function calls.
func (w *goWorker) run() {
	// 协程池运行中的协程数 +1
	w.pool.addRunning(1)
	// 既然是协程运行，那可能性是开启新协程
	go func() {
		defer func() {
			// 1. worker对象的协程执行完，将协程池中的运行中的worker数 -1
			w.pool.addRunning(-1)
			// 2. 将协程对应的 worker对象，放回 workerCache 对象池中。
			w.pool.workerCache.Put(w)
			// 3. 捕获panic
			if p := recover(); p != nil {
				if ph := w.pool.options.PanicHandler; ph != nil {
					ph(p)
				} else {
					w.pool.options.Logger.Printf("worker exits from panic: %v\n%s\n", p, debug.Stack())
				}
			}
			// Call Signal() here in case there are goroutines waiting for available workers.
			// 4. 唤醒阻塞等待的【提交任务的协程】
			w.pool.cond.Signal()
		}()
		// 5. 注意：这里是开启循环，一直从 worker 的 task channel 中获取任务并执行，知道 task channel 关闭。
		// 从 worker 的任务队列中获取任务，并执行任务
		for f := range w.task {
			if f == nil {
				return
			}
			f()
			// 6. 任务执行完，将 worker 重置。所谓重置，就是主要是将worker放回协程池的queue。
			// 很明显，task channel 中只会有一个 task
			if ok := w.pool.revertWorker(w); !ok {
				return
			}
		}
	}()
}

func (w *goWorker) finish() {
	w.task <- nil
}

func (w *goWorker) lastUsedTime() time.Time {
	return w.lastUsed
}

func (w *goWorker) inputFunc(fn func()) {
	// 如果 task channel是阻塞channel，这里就会被阻塞
	w.task <- fn
}

func (w *goWorker) inputParam(interface{}) {
	panic("unreachable")
}
