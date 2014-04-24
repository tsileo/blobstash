package backend

type Backend interface {
	Put(hash string, data []byte) (err error)
	Exists(hash string) bool
	Get(hash string) (data []byte, err error)
}