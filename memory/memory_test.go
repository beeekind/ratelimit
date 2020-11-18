package memory

import (
	"testing"
)

var (
	defaultKey     = "foo"
	defaultBackend = New()
)

func BenchmarkGetState(b *testing.B) {
	for i := 0; i < b.N; i++ {
		defaultBackend.GetState(defaultKey)
	}
}

func BenchmarkSetState(b *testing.B) {
	for i := 0; i < b.N; i++ {
		defaultBackend.SetState(defaultKey, 10, 10)
	}
}
