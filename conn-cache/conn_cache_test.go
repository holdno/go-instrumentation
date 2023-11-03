package conncache_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"testing"
	"time"

	conncache "github.com/holdno/go-instrumentation/conn-cache"
	"github.com/holdno/go-instrumentation/conn-cache/etc/greeter"
	"google.golang.org/grpc"
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

func TestConnCache(t *testing.T) {
	endpoint := "127.0.0.1:3332"

	srv := grpc.NewServer()
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	greeter.RegisterGreeterServer(srv, &Greeter{})
	go srv.Serve(lis)

	ccCache := conncache.NewConnCache[string, *conncache.GRPCConn[string]](10000, time.Second, func(ctx context.Context, s string) (*conncache.GRPCConn[string], error) {
		fmt.Println("start to gen new connect")
		cc, err := grpc.DialContext(ctx, s, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return conncache.NewGrpcConn[string](s, cc), nil
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
