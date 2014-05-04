package backend

type BlobHandler interface {
	Put(hash string, data []byte) (err error)
	Exists(hash string) bool
	Get(hash string) (data []byte, err error)
	Enumerate(chan<- string) error
}

