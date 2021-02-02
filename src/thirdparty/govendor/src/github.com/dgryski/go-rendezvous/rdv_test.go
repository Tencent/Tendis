package rendezvous

import (
	"hash/fnv"
	"testing"
)

func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func TestEmpty(t *testing.T) {
	r := New([]string{}, hashString)
	r.Lookup("hello")

}
