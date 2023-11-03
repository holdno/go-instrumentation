package conncache

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

type Conn[K comparable] interface {
	IsUsed() bool
	Ready() bool
	Close() error
	CacheKey() K
	Done()
	add()
}

type cacheValue[K comparable, T Conn[K]] struct {
	latestGetTime time.Time
	conn          T
}

type cacheResponse[K comparable, T Conn[K]] struct {
	cc  T
	err error
}

type connectionRequest[K comparable, T Conn[K]] struct {
	ctx          context.Context
	key          K
	responseChan chan<- *cacheResponse[K, T]
}

type expireRequest[K comparable, T Conn[K]] struct {
	key K
	cc  T
}

type BuildConnFunc[K comparable, T Conn[K]] func(context.Context, K) (T, error)

type connCache[K comparable, T Conn[K]] struct {
	ctx            context.Context
	cancel         context.CancelFunc
	expireTime     time.Duration
	genConn        BuildConnFunc[K, T]
	connections    *expirable.LRU[K, T]
	lowerConnCache map[K]*cacheValue[K, T]
	requests       chan *connectionRequest[K, T]
	expireRequests chan *expireRequest[K, T]
	maxConn        int
	now            time.Time
}

func NewConnCache[K comparable, T Conn[K]](maxConn int, expireTime time.Duration, newConn BuildConnFunc[K, T]) *connCache[K, T] {
	lowerCache := make(map[K]*cacheValue[K, T])
	expireRequests := make(chan *expireRequest[K, T], 100)
	c := &connCache[K, T]{
		genConn: newConn,
		connections: expirable.NewLRU[K, T](maxConn, func(key K, value T) {
			expireRequests <- &expireRequest[K, T]{
				key: key,
				cc:  value,
			}
		}, expireTime),
		expireTime:     expireTime,
		lowerConnCache: lowerCache,
		requests:       make(chan *connectionRequest[K, T]),
		expireRequests: expireRequests,
		maxConn:        maxConn,
		now:            time.Now(),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	go c.loop()
	return c
}

func (c *connCache[K, T]) GetConn(ctx context.Context, key K) (T, error) {
	resChan := make(chan *cacheResponse[K, T])
	var empt T
	select {
	case <-ctx.Done():
		return empt, ctx.Err()
	case c.requests <- &connectionRequest[K, T]{
		ctx:          ctx,
		key:          key,
		responseChan: resChan,
	}:
	}

	select {
	case <-ctx.Done():
		return empt, ctx.Err()
	case res := <-resChan:
		return res.cc, res.err
	}
}

func (c *connCache[K, T]) addToCache(key K, value T) {
	if _, exist := c.connections.Get(key); !exist {
		c.connections.Add(key, value)
	}
	if _, exist := c.lowerConnCache[key]; !exist {
		c.lowerConnCache[key] = &cacheValue[K, T]{
			latestGetTime: c.now,
			conn:          value,
		}
	}
}

func (c *connCache[K, T]) removeCache(key K) {
	if conn, exist := c.lowerConnCache[key]; exist {
		conn.conn.Close()
		delete(c.lowerConnCache, key)
	}
	c.connections.Remove(key)
}

func (c *connCache[K, T]) returnConn(req *connectionRequest[K, T], conn T) {
	conn.add()
	defer close(req.responseChan)
	select {
	case <-req.ctx.Done():
	case <-c.ctx.Done():
	case req.responseChan <- &cacheResponse[K, T]{
		cc: conn,
	}:
		return
	}
	conn.Done()
}

func (c *connCache[K, T]) loop() {
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				c.now = time.Now()
			case <-c.ctx.Done():
				return
			}
		}
	}()
	waiting := make(map[K][]*connectionRequest[K, T])
	finished := make(chan *cacheResponse[K, T])

	for {
		select {
		case expireReq := <-c.expireRequests:
			// 过期的同时有新的连接请求
			// 过期的同时有连接正在被持有
			value := c.lowerConnCache[expireReq.key]
			if value.latestGetTime.Before(time.Now().Add(-c.expireTime)) {
				if c.connections.Len() == c.maxConn || !value.conn.Ready() || !value.conn.IsUsed() {
					fmt.Println("remove", c.connections.Len() == c.maxConn, !value.conn.Ready(), !value.conn.IsUsed())
					c.removeCache(expireReq.key)
					continue
				}
			}

			c.addToCache(expireReq.key, value.conn)
		case req := <-c.requests:
			// 获取缓存之前就会过期
			// 所以过期时加锁即可防止这边穿透
			conn, exist := c.connections.Get(req.key)
			if !exist {
				lowerCacheValue, lowerExist := c.lowerConnCache[req.key]
				if lowerExist {
					exist = lowerExist
					conn = lowerCacheValue.conn
				}
			}

			if exist {
				if conn.Ready() {
					c.lowerConnCache[req.key].latestGetTime = c.now
					c.returnConn(req, conn)
					continue
				}
				c.removeCache(req.key)
			}

			if alreadyWaiting, ok := waiting[req.key]; ok {
				waiting[req.key] = append(alreadyWaiting, req)
				continue
			}

			waiting[req.key] = []*connectionRequest[K, T]{req}
			go c.buildNewConn(req, finished)
		case conn := <-finished:
			if cachedConn, exist := c.connections.Get(conn.cc.CacheKey()); exist {
				conn.cc.Close()
				conn.cc = cachedConn
				c.lowerConnCache[conn.cc.CacheKey()].latestGetTime = c.now
			} else if conn.cc.Ready() { // check connect is ready to use.
				c.addToCache(conn.cc.CacheKey(), conn.cc)
			}

			for _, client := range waiting[conn.cc.CacheKey()] {
				// Send it over if the client is still there. Abort otherwise.
				// This also aborts if the cache context gets cancelled.
				c.returnConn(client, conn.cc)
			}
			delete(waiting, conn.cc.CacheKey())
		case <-c.ctx.Done():
			close(finished)
			return
		}
	}
}

func (c *connCache[K, T]) buildNewConn(req *connectionRequest[K, T], respChan chan<- *cacheResponse[K, T]) {
	conn, err := c.genConn(req.ctx, req.key)
	select {
	case respChan <- &cacheResponse[K, T]{
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
