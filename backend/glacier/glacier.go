/*

Package glacier implement a write-only backend backed by AWS Glacier that upload blobs incrementally,
it uses a blobsfile as cache to buffer blobs.

The BlobFiles will be uploaded when triggered by PubSub (using SIGNAL server command).


Put/Exists/Enumerate requests are performed on the cache.
Get is disabled since it's a write-only backend.

*/
package glacier

import (
	"expvar"
	"fmt"
	"os"
	"log"
	_ "strings"

	"github.com/tsileo/blobstash/backend"

	"github.com/tsileo/blobstash/backend/blobsfile"
	"github.com/tsileo/blobstash/pubsub"
    _ "github.com/tsileo/blobstash/backend/glacier/util"
	"github.com/rdwilliamson/aws/glacier"
	"github.com/rdwilliamson/aws"
	_ "github.com/cznic/kv"
)

var (
	bytesUploaded   = expvar.NewMap("glacier-bytes-uploaded")
	bytesDownloaded = expvar.NewMap("glacier-bytes-downloaded")
	blobsUploaded   = expvar.NewMap("glacier-blobs-uploaded")
	blobsDownloaded = expvar.NewMap("glacier-blobs-downloaded")
)

type GlacierBackend struct {
	Vault string
	cache backend.BlobHandler
	con *glacier.Connection
	pubsub *pubsub.PubSub
//	db *kv.DB
}

func New(vault string, cache backend.BlobHandler) *GlacierBackend {
	log.Println("GlacierBackend: starting")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	if accessKey == "" || secretKey == "" {
		panic("S3_ACCESS_KEY or S3_SECRET_KEY not set")
	}
	con := glacier.NewConnection(secretKey, accessKey, aws.EU)
	//db, err := util.GetDB()
	//if err != nil {
	//	panic(fmt.Errorf("Error initializing DB at %v: %v", util.DBPath, err))
	//}
	glacierPubSub := pubsub.NewPubSub("glacier")
	b := &GlacierBackend{vault, cache, con, glacierPubSub}
	if err := con.CreateVault(vault); err != nil {
		panic(fmt.Errorf("Error creating vault: %v", err))
	}
	// Move this to glacier backend to trigger the done and add a client func
	glacierPubSub.Listen()
	go func(b *GlacierBackend) {
		for {
			<-glacierPubSub.Msgc
			log.Println("GlacierBackend: Upload triggered")
			if err := b.Upload(); err != nil {
				panic(fmt.Errorf("failed to upload %v", err))
			}
		}
	}(b)
	return b
}

func (backend *GlacierBackend) String() string {
	// TODO add the vault
	return fmt.Sprintf("glacier-%v-%v", backend.Vault, backend.cache.String())
}

func (backend *GlacierBackend) Close() {
	backend.cache.Close()
	//backend.db.Close()
}

func (backend *GlacierBackend) Done() error {
	return nil
}

func (backend *GlacierBackend) Upload() error {
	// TODO handle upload to Glacier
	log.Printf("GlacierBackend %+v Upload()", backend)
	bfBackend, err := backend.cache.(*blobsfile.BlobsFileBackend)
	if !err {
		panic(fmt.Errorf("GlacierBackend cache must be a BlobsFileBackend"))
	}
	ofiles := bfBackend.IterOpenFiles()
	for _, f := range ofiles {
		f.Seek(0, 0)
		archiveId, err := backend.con.UploadArchive(backend.Vault, f, f.Name())
		if err != nil {
			return fmt.Errorf("Error uploading archive: %v", err)
		}
		log.Printf("archiveId: %v", archiveId)
	}
	if err := backend.cache.Done(); err != nil {
		return err
	}
	return nil
}

// TODO a way to restore

func (backend *GlacierBackend) Put(hash string, data []byte) (err error) {
	if err := backend.cache.Put(hash, data); err != nil {
		return err
	}
	bytesUploaded.Add("total", int64(len(data)))
	blobsUploaded.Add("total", 1)
	bytesUploaded.Add(backend.String(), int64(len(data)))
	blobsUploaded.Add(backend.String(), 1)
	return
}

func (backend *GlacierBackend) Exists(hash string) bool {
	return backend.cache.Exists(hash)
}

func (backend *GlacierBackend) Get(hash string) (data []byte, err error) {
	panic("GlacierBackend is a write-only backend")
	return
}

func (backend *GlacierBackend) Enumerate(blobs chan<- string) error {
	// TODO(tsileo) enumerate over all backends with a map to check if already sent ?
	return backend.cache.Enumerate(blobs)
}
