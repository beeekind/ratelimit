package radix

import (
	"strconv"
	"testing"

	"github.com/mediocregopher/radix/v3"
)

var (
	poolOne, err = radix.NewPool("tcp", "localhost:6379", 100, radix.PoolConnFunc(ConnectionFunction))
	backendOne   = New(poolOne)
)

func ConnectionFunction(network, addr string) (radix.Conn, error) {
	return radix.Dial(network, addr, radix.DialAuthPass("password"), radix.DialSelectDB(0))
}

func BenchmarkSetState(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := backendOne.SetState(strconv.Itoa(i), 10, 10)
		if err != nil {
			b.Log(err.Error())
			b.FailNow()
		}
	}
}

func BenchmarkGetState(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, err := backendOne.GetState(strconv.Itoa(i))
		if err != nil {
			b.Log(err.Error())
			b.FailNow()
		}
	}
}
