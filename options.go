package ants

import "time"

// Option represents the optional function.
type Option func(opts *Options)

func loadOptions(options ...Option) *Options {
	opts := new(Options)
	for _, option := range options {
		option(opts)
	}
	return opts
}

// Options contains all options which will be applied when instantiating an ants pool.
type Options struct {
	// ExpiryDuration is a period for the scavenger goroutine to clean up those expired workers,
	// the scavenger scans all workers every `ExpiryDuration` and clean up those workers that haven't been
	// used for more than `ExpiryDuration`.
	// ExpiryDuration是一个时间段，用于清理那些过期的工作者协程，
	// 每隔ExpiryDuration时间段，清理协程池中那些超过ExpiryDuration时间段未被使用的协程。
	ExpiryDuration time.Duration

	// PreAlloc indicates whether to make memory pre-allocation when initializing Pool.
	// PreAlloc表示在初始化协程池时是否进行内存预分配。
	PreAlloc bool

	// Max number of goroutine blocking on pool.Submit.
	// 0 (default value) means no such limit.
	// MaxBlockingTasks表示在协程池中最多允许阻塞的任务数。
	// 0（默认值）表示没有此类限制。
	MaxBlockingTasks int

	// When Nonblocking is true, Pool.Submit will never be blocked.
	// ErrPoolOverload will be returned when Pool.Submit cannot be done at once.
	// When Nonblocking is true, MaxBlockingTasks is inoperative.
	// 当Nonblocking为true时，Pool.Submit将永远不会被阻塞。 但如果Pool.Submit无法立即完成时，将返回ErrPoolOverload。
	// 当Nonblocking为true时，MaxBlockingTasks无效。
	Nonblocking bool

	// PanicHandler is used to handle panics from each worker goroutine.
	// if nil, panics will be thrown out again from worker goroutines.
	// PanicHandler用于处理每个工作者协程的panic。
	// 如果为nil，则从工作者协程中再次抛出panic。
	PanicHandler func(interface{})

	// Logger is the customized logger for logging info, if it is not set,
	// default standard logger from log package is used.
	// Logger是用于记录信息的自定义记录器，如果未设置，
	// 则使用log包中的默认标准记录器。
	Logger Logger

	// When DisablePurge is true, workers are not purged and are resident.
	// 当DisablePurge为true时，工作者协程不会被清除，而是一直驻留在协程池中。
	DisablePurge bool
}

// WithOptions accepts the whole options config.
func WithOptions(options Options) Option {
	return func(opts *Options) {
		*opts = options
	}
}

// WithExpiryDuration sets up the interval time of cleaning up goroutines.
func WithExpiryDuration(expiryDuration time.Duration) Option {
	return func(opts *Options) {
		opts.ExpiryDuration = expiryDuration
	}
}

// WithPreAlloc indicates whether it should malloc for workers.
func WithPreAlloc(preAlloc bool) Option {
	return func(opts *Options) {
		opts.PreAlloc = preAlloc
	}
}

// WithMaxBlockingTasks sets up the maximum number of goroutines that are blocked when it reaches the capacity of pool.
func WithMaxBlockingTasks(maxBlockingTasks int) Option {
	return func(opts *Options) {
		opts.MaxBlockingTasks = maxBlockingTasks
	}
}

// WithNonblocking indicates that pool will return nil when there is no available workers.
func WithNonblocking(nonblocking bool) Option {
	return func(opts *Options) {
		opts.Nonblocking = nonblocking
	}
}

// WithPanicHandler sets up panic handler.
func WithPanicHandler(panicHandler func(interface{})) Option {
	return func(opts *Options) {
		opts.PanicHandler = panicHandler
	}
}

// WithLogger sets up a customized logger.
func WithLogger(logger Logger) Option {
	return func(opts *Options) {
		opts.Logger = logger
	}
}

// WithDisablePurge indicates whether we turn off automatically purge.
func WithDisablePurge(disable bool) Option {
	return func(opts *Options) {
		opts.DisablePurge = disable
	}
}
