package cbgrpc

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	"github.com/holdno/go-instrumentation/circuitbreaker"
)

func setup[T any](errCount uint32, ratio float64, onStateChange func(method string, from circuitbreaker.State, to circuitbreaker.State)) func(method string) func(req func() (T, error)) (T, error) {
	var (
		methodsTool = make(map[string]*circuitbreaker.CircuitBreaker[T])
		st          circuitbreaker.Settings
		lock        sync.Mutex
		initFunc    = func(method string) *circuitbreaker.CircuitBreaker[T] {
			st.Name = method
			st.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
				failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
				return counts.Requests >= errCount && failureRatio >= ratio
			}
			st.OnStateChange = onStateChange
			return circuitbreaker.NewCircuitBreaker[T](st)
		}
	)
	return func(method string) func(req func() (T, error)) (T, error) {
		lock.Lock()
		cb, exist := methodsTool[method]
		if !exist {
			cb = initFunc(method)
			methodsTool[method] = cb
		}
		lock.Unlock()
		return cb.Execute
	}
}

func UnaryClientInterceptor(errCount uint32, ratio float64, onStateChange func(method string, from circuitbreaker.State, to circuitbreaker.State)) grpc.UnaryClientInterceptor {
	executer := setup[error](errCount, ratio, onStateChange)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err, _ := executer(method)(func() (error, error) {
			err := invoker(ctx, method, req, reply, cc, opts...)
			return err, err
		})
		return err
	}
}

func UnaryServerInterceptor(errCount uint32, ratio float64, onStateChange func(method string, from circuitbreaker.State, to circuitbreaker.State)) grpc.UnaryServerInterceptor {
	executer := setup[any](errCount, ratio, onStateChange)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		return executer(info.FullMethod)(func() (any, error) {
			return handler(ctx, req)
		})
	}
}
