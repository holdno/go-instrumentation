package ratelimit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Bucket[T any] struct {
	limitr   RateLimiter
	taskChan chan T
	once     *sync.Once
	isClose  *atomic.Bool
}

func NewLeakyBucket[T any](limit int, period time.Duration, remoteLimiter *Limiter) *Bucket[T] {
	b := &Bucket[T]{
		limitr:   New(fmt.Sprintf("leakybucket_%d", limit), limit, period, remoteLimiter),
		taskChan: make(chan T, limit*2),
		once:     &sync.Once{},
		isClose:  &atomic.Bool{},
	}

	return b
}

func (b *Bucket[T]) Set(task T) {
	if !b.isClose.Load() {
		b.taskChan <- task
	}
}

func (b *Bucket[T]) Get() (T, bool) {
	for {
		if b.limitr.Allow() {
			break
		}
		time.Sleep(time.Millisecond * 20)
	}

	task, ok := <-b.taskChan
	return task, ok
}

func (b *Bucket[T]) Done() {
	b.isClose.Store(true)
	b.once.Do(func() {
		close(b.taskChan)
	})
}
