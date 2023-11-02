package conncache

import (
	"context"
	"sync"
	"time"

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

type expireRequest[K comparable] struct {
	key K
	cc  Conn[K]
}

type BuildConnFunc[K comparable, T Conn[K]] func(context.Context, K) (T, error)

type connCache[K comparable, T Conn[K]] struct {
	ctx            context.Context
	cancel         context.CancelFunc
	genConn        BuildConnFunc[K, T]
	connections    *expirable.LRU[K, Conn[K]]
	requests       chan *connectionRequest[K]
	expireRequests chan *expireRequest[K]
}

func NewConnCache[K comparable, T Conn[K]](newConn BuildConnFunc[K, T]) *connCache[K, T] {
	c := &connCache[K, T]{
		genConn:     newConn,
		connections: expirable.NewLRU[K, Conn[K]](128, func(key K, value Conn[K]) {}, time.Minute*30),
	}
	return c
}

func (c *connCache[K, T]) loop() {
	waiting := make(map[K][]*connectionRequest[K])
	finished := make(chan *cacheResponse[K])
	locker := make(map[K]*sync.Mutex)

	for {
		select {
		case expireReq := <-c.expireRequests:
			// 过期的同时有新的连接请求
			// 过期的同时有连接正在被持有
			//
			if !expireReq.cc.Ready() {
				expireReq.cc.Close()
			} else if locker[expireReq.key].TryLock() {
				// current not building new connection or builded
				if _, exist := c.connections.Get(expireReq.key); !exist {
					if expireReq.cc.IsUsed() {
						c.connections.Add(expireReq.key, expireReq.cc)
					}
				}
			} else {
				expireReq.cc.Close()
			}
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
			go c.buildNewConn(req, finished)
		case conn := <-finished:
			if conn.cc.Ready() { // first to check connect is ready to use.
				c.connections.Add(conn.cc.CacheKey(), conn.cc)
			}
			for _, client := range waiting[conn.cc.CacheKey()] {
				// Send it over if the client is still there. Abort otherwise.
				// This also aborts if the cache context gets cancelled.
				select {
				case client.responseChan <- conn:
				case <-client.ctx.Done():
				case <-c.ctx.Done():
				}
				close(client.responseChan)
			}
			delete(waiting, conn.cc.CacheKey())

		case <-c.ctx.Done():
			close(finished)
			return
		}
	}
}

func (c *connCache[K, T]) buildNewConn(req *connectionRequest[K], respChan chan *cacheResponse[K]) {
	conn, err := c.genConn(req.ctx, req.key)
	select {
	case respChan <- &cacheResponse[K]{
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
}
