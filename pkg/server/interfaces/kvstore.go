package interfaces

type KeyValueStore interface {
	StoreSingleValue(namespace string, key string, value string) error
	RetrieveSingleValue(namespace string, key string) (string, error)
}
