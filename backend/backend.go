package backend

// BlobHandler is the interface that defines 
// all the method a "blob backend" must implement.
type BlobHandler interface {
	Put(hash string, data []byte) (err error)
	Exists(hash string) bool
	Get(hash string) (data []byte, err error)
	Enumerate(chan<- string) error
	Close()
	String() string
}
