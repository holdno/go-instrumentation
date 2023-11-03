package conncache

import (
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type GRPCConn[K comparable] struct {
	key  K
	used *atomic.Int64
	cc   *grpc.ClientConn
}

func NewGrpcConn[K comparable](key K, cc *grpc.ClientConn) *GRPCConn[K] {
	gc := &GRPCConn[K]{
		key:  key,
		used: &atomic.Int64{},
		cc:   cc,
	}
	return gc
}

func (c *GRPCConn[K]) add() {
	c.used.Add(1)
}

func (c *GRPCConn[K]) Done() {
	c.used.Add(-1)
}

func (c *GRPCConn[K]) CacheKey() K {
	return c.key
}

func (c *GRPCConn[K]) Ready() bool {
	switch c.cc.GetState() {
	case connectivity.Shutdown:
		return false
	case connectivity.TransientFailure:
		return false
	default:
		return true
	}
}

func (c *GRPCConn[K]) IsUsed() bool {
	return c.used.Load() > 0
}

func (c *GRPCConn[K]) Close() error {
	return c.cc.Close()
}

func (c *GRPCConn[K]) ClientConn() *grpc.ClientConn {
	return c.cc
}
