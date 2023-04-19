package machines

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
)

type Query struct {
	Table  string
	RowKey string
}
