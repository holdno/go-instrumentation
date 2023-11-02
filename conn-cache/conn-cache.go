package conncache

import (
	"context"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

type Conn[K comparable] interface {
	IsUsed() bool
	Ready() bool
	Close() error
	CacheKey() K
}

type cacheResponse[K comparable] struct {
	cc  Conn[K]
	err error
}

type connectionRequest[K comparable] struct {
	ctx          context.Context
	key          K
	responseChan chan<- *cacheResponse[K]
}

type BuildConnFunc[K comparable, T Conn[K]] func(context.Context, K) (T, error)

type connCache[K comparable, T Conn[K]] struct {
	ctx         context.Context
	cancel      context.CancelFunc
	genConn     BuildConnFunc[K, T]
	connections *expirable.LRU[K, T]
	requests    chan *connectionRequest[K]
}

func NewConnCache[K comparable, T Conn[K]](newConn BuildConnFunc[K, T]) *connCache[K, T] {
	c := &connCache[K, T]{
		genConn: newConn,
	}
	return c
}

func (c *connCache[K, T]) loop() {
	waiting := make(map[K][]*connectionRequest[K])
	finished := make(chan *cacheResponse[K])

	for {
		select {
		case req := <-c.requests:
			if conn, exist := c.connections.Get(req.key); exist {
				select {
				case <-req.ctx.Done():
				case <-c.ctx.Done():
				case req.responseChan <- &cacheResponse[K]{
					cc: conn,
				}:
				}
				close(req.responseChan)
				continue
			}

			if alreadyWaiting, ok := waiting[req.key]; ok {
				waiting[req.key] = append(alreadyWaiting, req)
				continue
			}

			waiting[req.key] = []*connectionRequest[K]{req}
			go func() {
				conn, err := c.genConn(req.ctx, req.key)
				select {
				case finished <- &cacheResponse[K]{
					cc:  conn,
					err: err,
				}:
					return
				case <-c.ctx.Done():
				case <-req.ctx.Done():
				}
				if err != nil {
					conn.Close()
				}
			}()
		}
	}
}
