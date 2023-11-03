package connpool

import "google.golang.org/grpc/connectivity"

type FakeConn struct {
	state connectivity.State
}

func NewFakeConn(state connectivity.State) *FakeConn {
	return &FakeConn{
		state: state,
	}
}

func (f *FakeConn) GetState() connectivity.State {
	return f.state
}

func (f *FakeConn) Close() error {
	f.state = connectivity.Shutdown
	return nil
}
