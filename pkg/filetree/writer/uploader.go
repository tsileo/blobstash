package writer

import "context"

var (
	uploader    = 25 // concurrent upload uploaders
	dirUploader = 12 // concurrent directory uploaders
)

type BlobStorer interface {
	// Get(context.Context, string) ([]byte, error)
	// Enumerate(chan<- string, string, string, int) error
	Stat(string) (bool, error)
	Put(context.Context, string, []byte) error
}

type Uploader struct {
	bs BlobStorer

	uploader    chan struct{}
	dirUploader chan struct{}

	// Ignorer *gignore.GitIgnore
	Root string
}

func NewUploader(bs BlobStorer) *Uploader {
	return &Uploader{
		bs: bs,
		// kvs:         kvs,
		uploader:    make(chan struct{}, uploader),
		dirUploader: make(chan struct{}, dirUploader),
	}
}

// Block until the client can start the upload, thus limiting the number of file descriptor used.
func (up *Uploader) StartUpload() {
	up.uploader <- struct{}{}
}

// Read from the channel to let another upload start
func (up *Uploader) UploadDone() {
	select {
	case <-up.uploader:
	default:
		panic("No upload to wait for")
	}
}

// Block until the client can start the upload, thus limiting the number of file descriptor used.
func (up *Uploader) StartDirUpload() {
	up.dirUploader <- struct{}{}
}

// Read from the channel to let another upload start
func (up *Uploader) DirUploadDone() {
	select {
	case <-up.dirUploader:
	default:
		panic("No upload to wait for")
	}
}
