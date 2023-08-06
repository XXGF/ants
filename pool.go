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
	"context"
	"sync"
	"sync/atomic"
	"time"

	syncx "github.com/panjf2000/ants/v2/internal/sync"
)

// Pool accepts the tasks from client, it limits the total of goroutines to a given number by recycling goroutines.
type Pool struct {
	// capacity of the pool, a negative value means that the capacity of pool is limitless, an infinite pool is used to
	// avoid potential issue of endless blocking caused by nested usage of a pool: submitting a task to pool
	// which submits a new task to the same pool.
	// 容量
	capacity int32

	// running is the number of the currently running goroutines.
	// 当期运行中的协程数量
	running int32

	// lock for protecting the worker queue.
	// 这是个接口，可以接受所有的锁类型
	lock sync.Locker

	// workers is a slice that store the available workers.
	// 存储可用worker的队列
	workers workerQueue

	// state is used to notice the pool to closed itself.
	// 协程池状态：开启、关闭
	state int32

	// cond for waiting to get an idle worker.
	// 用于让当前协程等待空闲的 worker
	cond *sync.Cond

	// workerCache speeds up the obtainment of a usable worker in function:retrieveWorker.
	// sync.Pool是一个对象池，用于存储和复用临时对象。它可以提高程序的性能，减少内存分配和垃圾回收的开销。
	// 这里使用 sync.Pool 是为了提升获取可用worker的获取速度
	workerCache sync.Pool

	// waiting is the number of goroutines already been blocked on pool.Submit(), protected by pool.lock
	// 阻塞在 pool.Submit 方法调用的协程数
	waiting int32

	// 协程池清理相关
	purgeDone int32
	stopPurge context.CancelFunc

	// 定时器相关
	ticktockDone int32
	stopTicktock context.CancelFunc

	// 协程池的当前时间，为了并发安全，使用了 atomic.Value
	now atomic.Value

	// 配置项
	options *Options
}

// purgeStaleWorkers clears stale workers periodically, it runs in an individual goroutine, as a scavenger.
func (p *Pool) purgeStaleWorkers(ctx context.Context) {
	ticker := time.NewTicker(p.options.ExpiryDuration)

	defer func() {
		ticker.Stop()
		atomic.StoreInt32(&p.purgeDone, 1)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if p.IsClosed() {
			break
		}

		var isDormant bool
		p.lock.Lock()
		staleWorkers := p.workers.refresh(p.options.ExpiryDuration)
		n := p.Running()
		isDormant = n == 0 || n == len(staleWorkers)
		p.lock.Unlock()

		// Notify obsolete workers to stop.
		// This notification must be outside the p.lock, since w.task
		// may be blocking and may consume a lot of time if many workers
		// are located on non-local CPUs.
		for i := range staleWorkers {
			staleWorkers[i].finish()
			staleWorkers[i] = nil
		}

		// There might be a situation where all workers have been cleaned up(no worker is running),
		// while some invokers still are stuck in "p.cond.Wait()", then we need to awake those invokers.
		if isDormant && p.Waiting() > 0 {
			p.cond.Broadcast()
		}
	}
}

// ticktock is a goroutine that updates the current time in the pool regularly.
// 每 500 毫秒更新一次协程池的当前时间
func (p *Pool) ticktock(ctx context.Context) {
	// 每 500 毫秒执行一次的定时器
	ticker := time.NewTicker(nowTimeUpdateInterval)
	defer func() {
		ticker.Stop()
		atomic.StoreInt32(&p.ticktockDone, 1)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if p.IsClosed() {
			break
		}
		// 更新协程池的当前时间
		p.now.Store(time.Now())
	}
}

func (p *Pool) goPurge() {
	if p.options.DisablePurge {
		return
	}

	// Start a goroutine to clean up expired workers periodically.
	var ctx context.Context
	ctx, p.stopPurge = context.WithCancel(context.Background())
	go p.purgeStaleWorkers(ctx)
}

func (p *Pool) goTicktock() {
	p.now.Store(time.Now())
	var ctx context.Context
	ctx, p.stopTicktock = context.WithCancel(context.Background())
	go p.ticktock(ctx)
}

func (p *Pool) nowTime() time.Time {
	return p.now.Load().(time.Time)
}

// NewPool generates an instance of ants pool.
func NewPool(size int, options ...Option) (*Pool, error) {
	if size <= 0 {
		size = -1
	}

	opts := loadOptions(options...)

	if !opts.DisablePurge {
		if expiry := opts.ExpiryDuration; expiry < 0 {
			return nil, ErrInvalidPoolExpiry
		} else if expiry == 0 {
			opts.ExpiryDuration = DefaultCleanIntervalTime
		}
	}

	if opts.Logger == nil {
		opts.Logger = defaultLogger
	}

	p := &Pool{
		// 1. 容量
		capacity: int32(size),
		// 2. 自旋锁
		lock: syncx.NewSpinLock(),
		// 3. 配置项
		options: opts,
	}
	// 4. 指定对象池的默认对象。【对象池存储的是worker】
	p.workerCache.New = func() interface{} {
		// 协程池存储的默认对象
		return &goWorker{
			pool: p,
			// task channel 要么是阻塞channel，要么是容量为1的channel
			task: make(chan func(), workerChanCap),
		}
	}
	if p.options.PreAlloc {
		if size == -1 {
			return nil, ErrInvalidPreAllocSize
		}
		p.workers = newWorkerQueue(queueTypeLoopQueue, size)
	} else {
		p.workers = newWorkerQueue(queueTypeStack, 0)
	}

	p.cond = sync.NewCond(p.lock)

	// 开启新协程，定时清理协程池的worker
	p.goPurge()
	// 开启新协程，每500毫秒更新一次协程池的的当前时间
	p.goTicktock()

	return p, nil
}

// ---------------------------------------------------------------------------

// Submit submits a task to this pool.
// 向协程池中提交任务
//
// Note that you are allowed to call Pool.Submit() from the current Pool.Submit(),
// but what calls for special attention is that you will get blocked with the latest
// Pool.Submit() call once the current Pool runs out of its capacity, and to avoid this,
// you should instantiate a Pool with ants.WithNonblocking(true).
func (p *Pool) Submit(task func()) error {
	if p.IsClosed() {
		return ErrPoolClosed
	}
	// 1. 尝试获取 worker
	if w := p.retrieveWorker(); w != nil {
		// 2. 获取worker成功，将任务放到 worker 的task channel中，然后返回。接下来的任务的执行，由协程池中的协程去做。
		w.inputFunc(task)
		return nil
	}
	// 3. 这里如果没有获取到可用的worker，就报错
	return ErrPoolOverload
}

// Running returns the number of workers currently running.
func (p *Pool) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

// Free returns the number of available goroutines to work, -1 indicates this pool is unlimited.
func (p *Pool) Free() int {
	c := p.Cap()
	if c < 0 {
		return -1
	}
	return c - p.Running()
}

// Waiting returns the number of tasks which are waiting be executed.
func (p *Pool) Waiting() int {
	return int(atomic.LoadInt32(&p.waiting))
}

// Cap returns the capacity of this pool.
func (p *Pool) Cap() int {
	return int(atomic.LoadInt32(&p.capacity))
}

// Tune changes the capacity of this pool, note that it is noneffective to the infinite or pre-allocation pool.
func (p *Pool) Tune(size int) {
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity || p.options.PreAlloc {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
	if size > capacity {
		if size-capacity == 1 {
			p.cond.Signal()
			return
		}
		p.cond.Broadcast()
	}
}

// IsClosed indicates whether the pool is closed.
func (p *Pool) IsClosed() bool {
	return atomic.LoadInt32(&p.state) == CLOSED
}

// Release closes this pool and releases the worker queue.
func (p *Pool) Release() {
	if !atomic.CompareAndSwapInt32(&p.state, OPENED, CLOSED) {
		return
	}

	if p.stopPurge != nil {
		p.stopPurge()
		p.stopPurge = nil
	}
	p.stopTicktock()
	p.stopTicktock = nil

	p.lock.Lock()
	p.workers.reset()
	p.lock.Unlock()
	// There might be some callers waiting in retrieveWorker(), so we need to wake them up to prevent
	// those callers blocking infinitely.
	p.cond.Broadcast()
}

// ReleaseTimeout is like Release but with a timeout, it waits all workers to exit before timing out.
func (p *Pool) ReleaseTimeout(timeout time.Duration) error {
	if p.IsClosed() || (!p.options.DisablePurge && p.stopPurge == nil) || p.stopTicktock == nil {
		return ErrPoolClosed
	}
	p.Release()

	endTime := time.Now().Add(timeout)
	for time.Now().Before(endTime) {
		if p.Running() == 0 &&
			(p.options.DisablePurge || atomic.LoadInt32(&p.purgeDone) == 1) &&
			atomic.LoadInt32(&p.ticktockDone) == 1 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ErrTimeout
}

// Reboot reboots a closed pool.
func (p *Pool) Reboot() {
	if atomic.CompareAndSwapInt32(&p.state, CLOSED, OPENED) {
		atomic.StoreInt32(&p.purgeDone, 0)
		p.goPurge()
		atomic.StoreInt32(&p.ticktockDone, 0)
		p.goTicktock()
	}
}

// ---------------------------------------------------------------------------

func (p *Pool) addRunning(delta int) {
	atomic.AddInt32(&p.running, int32(delta))
}

func (p *Pool) addWaiting(delta int) {
	atomic.AddInt32(&p.waiting, int32(delta))
}

// retrieveWorker returns an available worker to run the tasks.
// 获取一个可用的worker
func (p *Pool) retrieveWorker() (w worker) {
	// 这个函数是从 workerCache 对象池中获取 worker 并 执行。
	spawnWorker := func() {
		// workerCache 如果没有预先分配内存，则这里获取的就是默认对象。
		// queue中没有可用的worker时，这里会新建一个 worker，并开启协程一直读worker的 task channel 中的任务，并执行任务。
		w = p.workerCache.Get().(*goWorker)
		w.run()
	}

	p.lock.Lock()
	// 1. 从queue中获取worker，并将worker出队，防止其他协程也拿到这个 worker。
	// 也就是说每个提交任务的协程，拿到的worker都是唯一的。
	w = p.workers.detach()
	if w != nil {
		// first try to fetch the worker from the queue
		// queue中取到了worker，直接返回
		p.lock.Unlock()
	} else if capacity := p.Cap(); capacity == -1 || capacity > p.Running() {
		// 2. 容量无限制，或容量大于正在运行的worker数量，则从 workerCache对象池中获取
		// if the worker queue is empty and we don't run out of the pool capacity,
		// then just spawn a new worker goroutine.
		p.lock.Unlock()
		// 从 workerCache 对象池中获取一个worker，并执行
		spawnWorker()
	} else {
		// otherwise, we'll have to keep them blocked and wait for at least one worker to be put back into pool.
		// 3. 正在运行的worker数量，已经大于协程池的capacity了。只能对请求获取worker的协程进行阻塞
		if p.options.Nonblocking {
			// 非阻塞协程池直接返回
			p.lock.Unlock()
			return
		}
		// 4. 这里是提交任务的协程阻塞 和 唤醒后重新竞争的逻辑。
	retry:
		if p.options.MaxBlockingTasks != 0 && p.Waiting() >= p.options.MaxBlockingTasks {
			// 4.1 阻塞等待的协程数，大于上限，直接返回
			p.lock.Unlock()
			return
		}
		// 4.2 添加等待的协程数
		p.addWaiting(1)
		// 4.3 提交任务的协程阻塞等待
		p.cond.Wait() // block and wait for an available worker
		// 4.4 提交任务的协程被唤醒【这里是全部唤醒，存在惊群效应，不会有性能问题吗？？？】
		p.addWaiting(-1)

		if p.IsClosed() {
			p.lock.Unlock()
			return
		}

		// 4.5 同样是：queue中获取不到，就去 worker Cache 中获取
		if w = p.workers.detach(); w == nil {
			if p.Free() > 0 {
				p.lock.Unlock()
				spawnWorker()
				return
			}
			// 还是获取不到，就重复上述流程，继续阻塞
			goto retry
		}
		// 4.6 从 queue中获取到了，直接返回
		p.lock.Unlock()
	}
	return
}

// revertWorker puts a worker back into free pool, recycling the goroutines.
func (p *Pool) revertWorker(worker *goWorker) bool {
	// 1. 协程池溢出或协程池关闭，唤醒所有提交任务的协程
	if capacity := p.Cap(); (capacity > 0 && p.Running() > capacity) || p.IsClosed() {
		p.cond.Broadcast()
		return false
	}
	// 2. 获取协程池的当前时间，作为 worker 的 lastUsed 时间
	worker.lastUsed = p.nowTime()

	p.lock.Lock()
	// To avoid memory leaks, add a double check in the lock scope.
	// Issue: https://github.com/panjf2000/ants/issues/113
	// 3. double check 协程池是否关闭
	if p.IsClosed() {
		p.lock.Unlock()
		return false
	}
	// 4. 将 worker 放回 queue
	if err := p.workers.insert(worker); err != nil {
		p.lock.Unlock()
		return false
	}
	// Notify the invoker stuck in 'retrieveWorker()' of there is an available worker in the worker queue.
	// 5. 唤醒阻塞等待的提交任务的协程
	p.cond.Signal()
	p.lock.Unlock()

	return true
}
