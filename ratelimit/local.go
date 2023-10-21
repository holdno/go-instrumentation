package ratelimit

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"
)

type rateLimiterWithLocalImpl struct {
	local         *rate.Limiter
	remote        *Limiter
	remoteLimiter Limit
	limit         int
	period        time.Duration
	key           string
}

func New(key string, limit int, period time.Duration, remoteLimiter *Limiter) RateLimiter {
	impl := &rateLimiterWithLocalImpl{
		local:  rate.NewLimiter(rate.Every(period), limit),
		remote: remoteLimiter,
		remoteLimiter: Limit{
			Period: period,
			Rate:   limit,
			Burst:  limit,
		},
		limit:  limit,
		period: period,
		key:    key,
	}
	return impl
}

type RateLimiter interface {
	Allow() bool
	AllowN(int) bool
}

func (r *rateLimiterWithLocalImpl) Allow() bool {
	if r.remote != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		if res, err := r.remote.Allow(ctx, r.key, r.remoteLimiter); err == nil {
			defer r.local.Allow()
			return res.Allowed == 1
		} else {
			fmt.Println(err.Error(), "downgrade")
		}
	}

	return r.local.Allow()
}

func (r *rateLimiterWithLocalImpl) AllowN(n int) bool {
	if r.remote != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		if res, err := r.remote.AllowN(ctx, r.key, r.remoteLimiter, n); err == nil {
			defer r.local.AllowN(time.Now(), n)
			return res.Allowed == n
		} else {
			fmt.Println(err.Error(), "downgrade")
		}
	}

	return r.local.AllowN(time.Now(), n)
}
