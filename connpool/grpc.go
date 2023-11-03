package connpool

import (
	"sync/atomic"

	"google.golang.org/grpc/connectivity"
)

type GrpcDriver interface {
	GetState() connectivity.State
	Close() error
}

type GRPCConn[K comparable, T GrpcDriver] struct {
	key  K
	cc   T
	used *atomic.Int64
}

func WrapGrpcConn[K comparable, T GrpcDriver](key K, value T) *GRPCConn[K, T] {
	gc := &GRPCConn[K, T]{
		key:  key,
		used: &atomic.Int64{},
		cc:   value,
	}
	return gc
}

func (c *GRPCConn[K, T]) add() {
	c.used.Add(1)
}

func (c *GRPCConn[K, T]) Done() {
	c.used.Add(-1)
}

func (c *GRPCConn[K, T]) CacheKey() K {
	return c.key
}

func (c *GRPCConn[K, T]) Ready() bool {
	switch c.cc.GetState() {
	case connectivity.Shutdown:
		return false
	case connectivity.TransientFailure:
		return false
	default:
		return true
	}
}

func (c *GRPCConn[K, T]) IsUsed() bool {
	return c.used.Load() > 0
}

func (c *GRPCConn[K, T]) Close() {
	c.cc.Close()
}

func (c *GRPCConn[K, T]) ClientConn() T {
	return c.cc
}
