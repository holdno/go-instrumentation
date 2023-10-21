package ratelimit_test

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/holdno/go-instrumentation/ratelimit"
)

func ExampleNew() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	_ = rdb.FlushDB(ctx).Err()
	limit := ratelimit.New("project:123", 10, time.Second*10, ratelimit.NewLimiter(rdb, ratelimit.KeyPrefix("yourprefix_")))

	fmt.Println(limit.AllowN(2))
	rdb.Close()
	fmt.Println(limit.AllowN(10))
	fmt.Println(limit.AllowN(8))

	// Output:
	// true
	// redis: client is closed downgrade
	// false
	// redis: client is closed downgrade
	// true
}

func ExampleNewLimiter() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	_ = rdb.FlushDB(ctx).Err()

	limiter := ratelimit.NewLimiter(rdb, ratelimit.KeyPrefix("yourprefix_"))

	res, err := limiter.Allow(ctx, "project:123", ratelimit.Limit{
		Rate:   1,
		Burst:  1,
		Period: time.Second * 10,
	})
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Second * 10)

	res, err = limiter.Allow(ctx, "project:123", ratelimit.Limit{
		Rate:   10,
		Burst:  10,
		Period: time.Second * 10,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("allowed", res.Allowed, "remaining", res.Remaining)

	// Output: allowed 1 remaining 9
}
