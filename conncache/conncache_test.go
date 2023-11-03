package conncache_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holdno/go-instrumentation/conncache"
	"github.com/holdno/go-instrumentation/conncache/etc/greeter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type Greeter struct {
	greeter.UnimplementedGreeterServer
}

func (g *Greeter) SayHello(ctx context.Context, req *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{
		Message: "hi",
	}, nil
}

func startServe() string {
	endpoint := "127.0.0.1:3332"

	srv := grpc.NewServer()
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	greeter.RegisterGreeterServer(srv, &Greeter{})
	go srv.Serve(lis)
	return endpoint
}

func TestConnCache(t *testing.T) {
	endpoint := startServe()
	ccCache := conncache.NewConnCache[string, *conncache.GRPCConn[string, *grpc.ClientConn]](10000, time.Second, func(ctx context.Context, s string) (*conncache.GRPCConn[string, *grpc.ClientConn], error) {
		fmt.Println("gen new connect", s)
		cc, err := grpc.DialContext(ctx, s, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return conncache.WrapGrpcConn[string, *grpc.ClientConn](s, cc), nil
	}, func(s string, r conncache.RemoveReason) {
		fmt.Println(s, "removed", r.Reason())
	})

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		cc, err := ccCache.GetConn(ctx, endpoint)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

		client := greeter.NewGreeterClient(cc.ClientConn())
		reqCtx, reqCancel := context.WithTimeout(context.Background(), time.Second)
		defer reqCancel()
		resp, err := client.SayHello(reqCtx, &greeter.HelloRequest{
			Name: "xiaoming",
		})
		if err != nil {
			t.Fatal(err)
		}
		cc.Done()
		fmt.Println(resp.Message)
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
	}

	time.Sleep(time.Second)
}

func genRandKeyFunc() func() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	getKey := func() string {
		b := make([]rune, 12)
		for i := range b {
			b[i] = letterRunes[r.Intn(len(letterRunes))]
		}
		return string(b)
	}
	return getKey
}

func TestTimeout(t *testing.T) {
	getKey := genRandKeyFunc()
	ccCache := conncache.NewConnCache[string, *conncache.GRPCConn[string, *conncache.FakeConn]](100, time.Second, func(ctx context.Context, s string) (*conncache.GRPCConn[string, *conncache.FakeConn], error) {
		cc := conncache.NewFakeConn(connectivity.Ready)
		return conncache.WrapGrpcConn[string, *conncache.FakeConn](s, cc), nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*5)
	defer cancel()

	expectError := context.DeadlineExceeded
	var latestError error
	for i := 0; i < 5; i++ {
		_, err := ccCache.GetConn(ctx, getKey())
		if err != nil {
			latestError = err
			break
		}
		time.Sleep(time.Millisecond * 2)
	}

	if latestError != expectError {
		t.Fatal("unexpected")
	}
}

func TestDialError(t *testing.T) {
	getKey := genRandKeyFunc()
	expectError := errors.New("dial error")
	ccCache := conncache.NewConnCache[string, *conncache.GRPCConn[string, *conncache.FakeConn]](100, time.Second, func(ctx context.Context, s string) (*conncache.GRPCConn[string, *conncache.FakeConn], error) {
		return nil, expectError
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*5)
	defer cancel()

	var latestError error
	for i := 0; i < 5; i++ {
		_, err := ccCache.GetConn(ctx, getKey())
		if err != nil {
			latestError = err
			break
		}
		time.Sleep(time.Millisecond * 2)
	}

	if latestError != expectError {
		t.Fatal("unexpected")
	}
}

func BenchmarkGetConn(b *testing.B) {
	getKey := genRandKeyFunc()
	ccCache := conncache.NewConnCache[string, *conncache.GRPCConn[string, *conncache.FakeConn]](100, time.Second, func(ctx context.Context, s string) (*conncache.GRPCConn[string, *conncache.FakeConn], error) {
		cc := conncache.NewFakeConn(connectivity.Ready)
		return conncache.WrapGrpcConn[string, *conncache.FakeConn](s, cc), nil
	}, nil)

	ctx := context.Background()

	keys := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		keys = append(keys, getKey())
	}

	for i := 0; i < b.N; i++ {
		ccCache.GetConn(ctx, keys[rand.Intn(100)%50])
	}
}

func TestMultiKeys(t *testing.T) {
	getKey := genRandKeyFunc()

	var removedCount atomic.Int64
	ccCache := conncache.NewConnCache[string, *conncache.GRPCConn[string, *conncache.FakeConn]](100, time.Second, func(ctx context.Context, s string) (*conncache.GRPCConn[string, *conncache.FakeConn], error) {
		fmt.Println("new", s)
		cc := conncache.NewFakeConn(connectivity.Ready)
		return conncache.WrapGrpcConn[string, *conncache.FakeConn](s, cc), nil
	}, func(s string, reason conncache.RemoveReason) {
		fmt.Println(s, "removed", reason.Reason())
		removedCount.Add(1)
	})

	getOne := func() *conncache.FakeConn {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		cc, err := ccCache.GetConn(ctx, getKey())
		if err != nil {
			t.Fatal(err)
		}
		return cc.ClientConn()
	}

	for i := 0; i < 120; i++ {
		getOne()
	}

	time.Sleep(time.Second)

	if removedCount.Load() != 20 {
		t.Fatal("unexpect")
	}
}
