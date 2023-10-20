package ratelimit_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/holdno/go-instrumentation/ratelimit"
)

func ExampleNewLimiter() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	_ = rdb.FlushDB(ctx).Err()

	limiter := ratelimit.NewLimiter(rdb, ratelimit.KeyPrefix("yourprefix_"))
	res, err := limiter.Allow(ctx, "project:123", ratelimit.PerSecond(10))
	if err != nil {
		panic(err)
	}
	fmt.Println("allowed", res.Allowed, "remaining", res.Remaining)
	// Output: allowed 1 remaining 9
}
