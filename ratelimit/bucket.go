package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

type Bucket[T any] struct {
	closeChan chan struct{}
	limitr    RateLimiter
	taskChan  chan T
	once      *sync.Once
	isClose   *atomic.Bool
}

func (b *Bucket[T]) Closed() chan struct{} {
	return b.closeChan
}

func NewLeakyBucket[T any](key string, limit int, period time.Duration, remoteLimiter *Limiter) *Bucket[T] {
	b := &Bucket[T]{
		limitr:    New(key, limit, period, remoteLimiter),
		taskChan:  make(chan T, limit*2),
		closeChan: make(chan struct{}),
		once:      &sync.Once{},
		isClose:   &atomic.Bool{},
	}

	return b
}

func (b *Bucket[T]) Set(task T) {
	if !b.isClose.Load() {
		select {
		case <-b.closeChan:
		case b.taskChan <- task:
		}
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
		close(b.closeChan)
	})
}
