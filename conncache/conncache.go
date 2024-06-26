package conncache

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

type Logger interface {
	Info(s string)
	Error(s string)
}

type ConnCache[K comparable, T Conn[K]] interface {
	GetConn(ctx context.Context, key K) (T, error)
	GetQueueLoad() map[K]int
	Len() int
}

type Conn[K comparable] interface {
	IsUsed() bool
	Ready() bool
	Close()
	CacheKey() K
	Done()
	add()
}

type cacheValue[K comparable, T Conn[K]] struct {
	latestGetTime time.Time
	conn          T
}

type cacheResponse[K comparable, T Conn[K]] struct {
	key K
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
	ctx              context.Context
	cancel           context.CancelFunc
	expireTime       time.Duration
	genConn          BuildConnFunc[K, T]
	connections      *expirable.LRU[K, T]
	lowerConnCache   map[K]*cacheValue[K, T]
	requests         chan *connectionRequest[K, T]
	expireRequests   chan *expireRequest[K, T]
	queueLoadRequest chan chan map[K]int
	maxConn          int
	now              time.Time
	logger           Logger

	onAdded  func(K, AddReason)
	onRemove func(K, RemoveReason)
}

type Option[K comparable, T Conn[K]] func(c *connCache[K, T])

func WithLogger[K comparable, T Conn[K]](l Logger) Option[K, T] {
	return func(c *connCache[K, T]) {
		c.logger = l
	}
}

func NewConnCache[K comparable, T Conn[K]](maxConn int, expireTime time.Duration, newConn BuildConnFunc[K, T], onAdded func(K, AddReason), onRemove func(K, RemoveReason), opts ...Option[K, T]) *connCache[K, T] {
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
		expireTime:       expireTime,
		lowerConnCache:   lowerCache,
		requests:         make(chan *connectionRequest[K, T]),
		expireRequests:   expireRequests,
		queueLoadRequest: make(chan chan map[K]int),
		maxConn:          maxConn,
		now:              time.Now(),
		onAdded:          onAdded,
		onRemove:         onRemove,
		logger:           &NopLogger{},
	}

	for _, opt := range opts {
		opt(c)
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	go c.loop()
	return c
}

func (c *connCache[K, T]) Len() int {
	return c.connections.Len()
}

func (c *connCache[K, T]) GetConn(ctx context.Context, key K) (T, error) {
	resChan := make(chan *cacheResponse[K, T])
	var empt T
	select {
	case <-ctx.Done():
		c.logger.Error(LOGGER_ERROR_STRING_REQUEST_ERROR)
		return empt, fmt.Errorf("request error: %w", ctx.Err())
	case c.requests <- &connectionRequest[K, T]{
		ctx:          ctx,
		key:          key,
		responseChan: resChan,
	}:
	}

	select {
	case <-ctx.Done():
		c.logger.Error(LOGGER_ERROR_STRING_RESPONSE_ERROR)
		return empt, fmt.Errorf("response error: %w", ctx.Err())
	case res := <-resChan:
		return res.cc, res.err
	}
}

func (c *connCache[K, T]) addToCache(key K, value T, reason AddReason) {
	c.lowerConnCache[key] = &cacheValue[K, T]{
		latestGetTime: c.now,
		conn:          value,
	}
	c.connections.Add(key, value)
	if c.onAdded != nil {
		go c.onAdded(key, reason)
	}
}

type AddReason string

const (
	ADD_REASON_NEW   AddReason = "new"
	ADD_REASON_USING AddReason = "using"

	LOGGER_STRING_NEW_REQUEST          = "new request"
	LOGGER_STRING_ON_EXPIRED           = "on expired"
	LOGGER_STRING_ON_CONNECTED         = "on connected"
	LOGGER_ERROR_STRING_REQUEST_ERROR  = "failed to request"
	LOGGER_ERROR_STRING_RESPONSE_ERROR = "failed to response"

	REASON_BAD_CONNECTION = "bas connection"
	REASON_EVICT          = "evict"
	REASON_EXPIRED        = "expired"
	REASON_UNKNOWN        = "unknown"
)

type RemoveReason struct {
	Expired       bool
	Evict         bool
	BadConnection bool
}

func (r RemoveReason) Reason() string {
	if r.BadConnection {
		return REASON_BAD_CONNECTION
	}
	if r.Evict {
		return REASON_EVICT
	}
	if r.Expired {
		return REASON_EXPIRED
	}
	return REASON_UNKNOWN
}

func (c *connCache[K, T]) removeCache(key K, reason RemoveReason) {
	if value, exist := c.lowerConnCache[key]; exist {
		go value.conn.Close()
		delete(c.lowerConnCache, key)
	}
	c.connections.Remove(key)
	if c.onRemove != nil {
		go c.onRemove(key, reason)
	}
}

func (c *connCache[K, T]) response(req *connectionRequest[K, T], conn *cacheResponse[K, T]) {
	if conn.err == nil {
		conn.cc.add()
	}
	defer close(req.responseChan)
	select {
	case <-req.ctx.Done():
	case <-c.ctx.Done():
	case req.responseChan <- conn:
		return
	}
	if conn.err == nil {
		conn.cc.Done()
	}
}

func (c *connCache[K, T]) GetQueueLoad() map[K]int {
	response := make(chan map[K]int)

	select {
	case <-c.ctx.Done():
		return map[K]int{}
	case c.queueLoadRequest <- response:
	}

	select {
	case <-c.ctx.Done():
		return map[K]int{}
	case res := <-response:
		return res
	}
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
		case resChan := <-c.queueLoadRequest:
			result := make(map[K]int)
			for k, v := range waiting {
				result[k] = len(v)
			}
			resChan <- result
			close(resChan)
		case expireReq := <-c.expireRequests:
			c.logger.Info(LOGGER_STRING_ON_EXPIRED)
			// 过期的同时有新的连接请求
			// 过期的同时有连接正在被持有
			value, exist := c.lowerConnCache[expireReq.key]
			if !exist {
				// 获取链接时，如果链接状态异常，会主动调用removeCache, removeCache会移除lowerConnCache中的元素，同时删除connects中的缓存，
				// 而删除connects缓存会再次触发该方法，所以需要检测移除内容是否已经完成移除
				continue
			}

			expired := value.latestGetTime.Before(time.Now().Add(-c.expireTime))
			evict := c.connections.Len() >= c.maxConn
			if evict || !value.conn.Ready() || (expired && !value.conn.IsUsed()) {
				c.removeCache(expireReq.key, RemoveReason{
					Expired:       expired,
					Evict:         evict,
					BadConnection: !value.conn.Ready(),
				})
				continue
			}

			if _, exist = c.connections.Get(expireReq.key); !exist {
				c.addToCache(expireReq.key, value.conn, ADD_REASON_USING)
			}
		case req := <-c.requests:
			c.logger.Info(LOGGER_STRING_NEW_REQUEST)
			conn, exist := c.connections.Get(req.key)
			if !exist { // 不存在可能是刚好触发了lru的过期
				lowerCacheValue, lowerExist := c.lowerConnCache[req.key]
				if lowerExist {
					exist = lowerExist
					conn = lowerCacheValue.conn
				}
			}

			if exist {
				if conn.Ready() {
					c.lowerConnCache[req.key].latestGetTime = c.now
					c.response(req, &cacheResponse[K, T]{
						key: req.key,
						cc:  conn,
					})
					continue
				}
				c.removeCache(req.key, RemoveReason{
					BadConnection: true,
				})
			}

			if alreadyWaiting, ok := waiting[req.key]; ok {
				waiting[req.key] = append(alreadyWaiting, req)
				continue
			}

			waiting[req.key] = []*connectionRequest[K, T]{req}
			go c.buildNewConn(req, finished)
		case conn := <-finished:
			c.logger.Info(LOGGER_STRING_ON_CONNECTED)
			c.addToCache(conn.key, conn.cc, ADD_REASON_NEW)

			for _, client := range waiting[conn.key] {
				// Send it over if the client is still there. Abort otherwise.
				// This also aborts if the cache context gets cancelled.
				c.response(client, conn)
			}
			delete(waiting, conn.key)
		case <-c.ctx.Done():
			close(finished)
			return
		}
	}
}

func (c *connCache[K, T]) buildNewConn(req *connectionRequest[K, T], respChan chan<- *cacheResponse[K, T]) {
	conn, err := c.genConn(req.ctx, req.key)
	select {
	case <-c.ctx.Done():
	default:
		respChan <- &cacheResponse[K, T]{
			key: req.key,
			cc:  conn,
			err: err,
		}
		return
	}

	if err == nil {
		go conn.Close()
	}
}
