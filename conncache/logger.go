package conncache

import "fmt"

type NopLogger struct{}

func (*NopLogger) Info(string)  {}
func (*NopLogger) Error(string) {}

type StdLogger struct{}

func (s *StdLogger) Info(str string) {
	fmt.Println("INFO: " + str)
}
func (s *StdLogger) Error(str string) {
	fmt.Println("ERROR: " + str)
}
