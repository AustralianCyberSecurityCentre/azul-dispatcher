package dedupe

import (
	"math/rand"
	"testing"
)

var result bool

func TestLookup(t *testing.T) {
	l := New(16)
	shouldNotContain(t, "Empty filter", l, []byte("aaaaaa"))
	shouldContain(t, "Last set", l, []byte("aaaaaa"))
	shouldNotContain(t, "New non colliding value", l, []byte("bbbbbb"))
	shouldContain(t, "Still set", l, []byte("aaaaaa"))
	shouldContain(t, "Last set", l, []byte("bbbbbb"))
	shouldNotContain(t, "New colliding value", l, []byte("cccccc"))
	shouldNotContain(t, "New colliding value", l, []byte("dddddd"))
	shouldNotContain(t, "Was evicted", l, []byte("bbbbbb"))
}

func BenchmarkLookup(b *testing.B) {
	l := New(100000)
	var seed [1000][]byte
	for i := 0; i < len(seed); i++ {
		seed[i] = make([]byte, 200)
		rand.Read(seed[i])
	}
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		val := seed[rand.Intn(len(seed))]
		result = l.CheckAndSet(val)
	}
}

func shouldContain(t *testing.T, msg string, l *Lookup, val []byte) {
	if !l.CheckAndSet(val) {
		t.Errorf("should contain, %s: val %v, array: %v", msg, val, l.keys)
	}
}

func shouldNotContain(t *testing.T, msg string, l *Lookup, val []byte) {
	if l.CheckAndSet(val) {
		t.Errorf("should not contain, %s: %v", msg, val)
	}
}
