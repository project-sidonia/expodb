package server

import (
	"hash/fnv"
)

type myMember string

func (m myMember) String() string {
	return string(m)
}

// consistent package doesn't provide a default hashing function.
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	f := fnv.New64a()
	f.Write(data)
	return f.Sum64()
}
