/*

http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-config-files


*/
package s3 // import "a4.io/blobstash/pkg/backend/s3"

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/inconshreveable/log15"

	"golang.org/x/crypto/nacl/secretbox"

	"a4.io/blobstash/pkg/backend"
	"a4.io/blobstash/pkg/backend/s3/index"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/hashutil"
	"a4.io/blobstash/pkg/queue"
)

var ErrWriteOnly = errors.New("backend is in read-only mode")

// The length of the nonce used for the secretbox implementation.
const nonceLength = 24

// The length of the encryption key for the secretbox implementation.
const keyLength = 32

// Set a flag to identify the encryption algorithm in case we support/switch encryption scheme later
const (
	naclSecretBox byte = 1 << iota
)

var (
	blobHeader = []byte("#blobstash/secretbox\n")
)

type Bucket struct {
	s3   *s3.S3
	Name string
}

func NewBucket(svc *s3.S3, name string) *Bucket {
	return &Bucket{s3: svc, Name: name}
}

func (b *Bucket) Create() error {
	params := &s3.CreateBucketInput{
		Bucket: aws.String(b.Name),
	}
	_, err := b.s3.CreateBucket(params)
	if err != nil {
		return err
	}
	return nil
}

func (b *Bucket) Exists() (bool, error) {
	params := &s3.HeadBucketInput{
		Bucket: aws.String(b.Name),
	}
	_, err := b.s3.HeadBucket(params)

	if err == nil {
		return true, nil
	}
	if errf, ok := err.(awserr.RequestFailure); ok && errf.StatusCode() == 404 {
		return false, nil
	}
	return false, err
}

func (b *Bucket) List(marker string, max int) ([]*Object, error) {
	var out []*Object
	params := &s3.ListObjectsInput{
		Bucket:    aws.String(b.Name),
		Delimiter: aws.String("/"),
		Marker:    aws.String(marker),
		MaxKeys:   aws.Int64(int64(max)),
	}
	resp, err := b.s3.ListObjects(params)

	if err != nil {
		return nil, err
	}

	for _, item := range resp.Contents {
		out = append(out, &Object{
			s3:     b.s3,
			Key:    *item.Key,
			Bucket: b.Name,
			Size:   *item.Size,
		})
	}
	return out, nil
}

func (b *Bucket) Iter(max int, f func(*Object) error) error {
	var marker string
	for {
		objects, err := b.List(marker, max)
		if err != nil {
			return err
		}

		if len(objects) == 0 {
			break
		}

		for _, object := range objects {
			if err := f(object); err != nil {
				return err
			}
			marker = nextKey(object.Key)
		}
	}

	return nil
}

type Object struct {
	Key    string
	Bucket string
	Size   int64
	s3     *s3.S3
}

func (o *Object) Peeker(size int64) (io.ReadCloser, error) {
	return o.reader(size)
}

func (o *Object) Reader() (io.ReadCloser, error) {
	return o.reader(-1)
}

func (o *Object) reader(size int64) (io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(o.Bucket),
		Key:    aws.String(o.Key),
	}
	if size > 0 {
		params.Range = aws.String(fmt.Sprintf("bytes=0-%d", size-1))
	}
	resp, err := o.s3.GetObject(params)

	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

type EncryptedBlob struct {
	o   *Object
	key *[32]byte
}

func NewEncryptedBlob(o *Object, key *[32]byte) *EncryptedBlob {
	return &EncryptedBlob{o: o, key: key}
}

func (b *EncryptedBlob) PlainText() ([]byte, error) {
	r, err := b.o.Reader()
	defer r.Close()
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(blobHeader, data[0:21]) {
		return nil, fmt.Errorf("missing header (\"%s\")", data[0:21])
	}

	decoded, err := open(b.key, data)
	if err != nil {
		return nil, err
	}

	return decoded, nil
}

func (b *EncryptedBlob) PlainTextHash() (string, error) {
	r, err := b.o.Peeker(53)
	defer r.Close()
	if err != nil {
		return "", err
	}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return "", err
	}

	if !bytes.Equal(blobHeader, data[0:21]) {
		return "", fmt.Errorf("missing header (\"%s\")", data[0:21])
	}

	return hex.EncodeToString(data[21:53]), nil
}

// Seal the data with the key derived from `password` (using scrypt) and seal the data with nacl/secretbox
func seal(nkey *[32]byte, hash string, data []byte) ([]byte, error) {
	nonce := new([nonceLength]byte)
	if _, err := rand.Reader.Read(nonce[:]); err != nil {
		return nil, err
	}
	bhash, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}
	// Box will contains our meta data (alg byte + salt + nonce)
	box := make([]byte, nonceLength+len(blobHeader)+len(bhash))
	copy(box[:], blobHeader)
	copy(box[len(blobHeader):], bhash)
	// And the nonce
	copy(box[len(blobHeader)+len(bhash):], nonce[:])
	return secretbox.Seal(box, data, nonce, nkey), nil
}

// Open a previously sealed secretbox with the key derived from `password` (using scrypt)
func open(nkey *[32]byte, data []byte) ([]byte, error) {
	// Extract the nonce
	nonce := new([nonceLength]byte)
	copy(nonce[:], data[len(blobHeader)+32:(len(blobHeader)+32+nonceLength)])
	box := data[(nonceLength + 32 + len(blobHeader)):]
	// Actually decrypt the cipher text
	decrypted, success := secretbox.Open(nil, box, nonce, nkey)

	// Ensure the decryption succeed
	if !success {
		return nil, errors.New("failed to decrypt file (bad password?)")
	}

	return decrypted, nil
}

type S3Backend struct {
	log log.Logger

	queue *queue.Queue
	index *index.Index

	encrypted bool
	key       *[32]byte

	backend backend.BlobHandler

	wg sync.WaitGroup

	s3   *s3.S3
	stop chan struct{}

	bucket string
}

func New(logger log.Logger, back backend.BlobHandler, conf *config.Config) (*S3Backend, error) {
	// Parse config
	bucket := conf.S3Repl.Bucket
	region := conf.S3Repl.Region
	scanMode := conf.S3ScanMode
	key, err := conf.S3Repl.Key()
	if err != nil {
		return nil, err
	}

	// Create a S3 Session
	sess := session.New(&aws.Config{Region: aws.String(region)})

	// Init the disk-backed queue
	q, err := queue.New(filepath.Join(conf.VarDir(), "s3-repl.queue"))
	if err != nil {
		return nil, err
	}

	// Init the disk-backed index
	indexPath := filepath.Join(conf.VarDir(), "s3-backend.index")
	if scanMode {
		logger.Debug("trying to remove old index file")
		os.Remove(indexPath)
	}
	i, err := index.New(indexPath)
	if err != nil {
		return nil, err
	}

	s3backend := &S3Backend{
		log:     logger,
		backend: back,
		s3:      s3.New(sess),
		stop:    make(chan struct{}),
		bucket:  bucket,
		key:     key,
		queue:   q,
		index:   i,
	}

	// FIXME(tsileo): should encypption be optional?
	if key != nil {
		s3backend.encrypted = true
	}

	logger.Info("Initializing S3 replication", "bucket", bucket, "encrypted", s3backend.encrypted, "scan_mode", scanMode)

	// Ensure the bucket exist
	obucket := NewBucket(s3backend.s3, bucket)
	ok, err := obucket.Exists()
	if err != nil {
		return nil, err
	}

	// Create it if it does not
	if !ok {
		logger.Info("creating bucket", "bucket", bucket)
		if err := obucket.Create(); err != nil {
			return nil, err
		}
	}

	// Trigger a re-indexing if requested
	if scanMode {
		if err := s3backend.reindex(obucket); err != nil {
			return nil, err
		}
	}

	// Initialize the worker (queue consumer)
	go s3backend.worker()

	return s3backend, nil
}

func (b *S3Backend) String() string {
	suf := ""
	if b.encrypted {
		suf = "-encrypted"
	}
	return fmt.Sprintf("s3-backend-%s", b.bucket) + suf
}

func (b *S3Backend) Put(hash string) error {
	return b.queue.Enqueue(&blob.Blob{Hash: hash})
}

// nextKey returns the next key for lexigraphical (key = NextKey(lastkey))
func nextKey(key string) string {
	bkey := []byte(key)
	i := len(bkey)
	for i > 0 {
		i--
		bkey[i]++
		if bkey[i] != 0 {
			break
		}
	}
	return string(bkey)
}

func (b *S3Backend) reindex(bucket *Bucket) error {
	b.log.Info("Starting S3 re-indexing")
	start := time.Now()
	max := 100
	cnt := 0

	if err := bucket.Iter(max, func(object *Object) error {
		b.log.Debug("fetching an objects batch from S3")
		blob := NewEncryptedBlob(object, nil)
		hash, err := blob.PlainTextHash()
		if err != nil {
			return err
		}
		b.log.Debug("indexing plain-text hash", "hash", hash)

		if err := b.index.Index(hash); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	b.log.Info("Sync done", "objects_cnt", cnt, "duration", time.Since(start))

	return nil
}

func (b *S3Backend) worker() {
	b.log.Debug("starting worker")
	t := time.NewTicker(30 * time.Second)
L:
	for {
		select {
		case <-b.stop:
			t.Stop()
			break L
		case <-t.C:
			b.log.Debug("repl tick")
			blb := &blob.Blob{}
			for {
				b.log.Debug("try to dequeue")
				ok, deqFunc, err := b.queue.Dequeue(blb)
				if err != nil {
					panic(err)
				}
				if ok {
					if err := func(blob *blob.Blob) error {
						t := time.Now()
						b.wg.Add(1)
						defer b.wg.Done()
						data, err := b.backend.Get(blob.Hash)
						if err != nil {
							deqFunc(false)
							return err
						}
						if err := b.put(blob.Hash, data); err != nil {
							deqFunc(false)
							return err
						}
						deqFunc(true)
						b.log.Info("blob uploaded to s3", "hash", blob.Hash, "duration", time.Since(t))

						return nil
					}(blb); err != nil {
						b.log.Error("failed to upload blob", "hash", blb.Hash, "err", err)
						time.Sleep(1 * time.Second)
					}
					continue
				}
				break
			}
		}
	}
}

func (b *S3Backend) put(hash string, data []byte) error {
	// Double check the blob does not exists
	exists, err := b.index.Exists(hash)
	if err != nil {
		return err
	}
	if exists {
		b.log.Debug("blob already exist", "hash", hash)
		return nil
	}

	// Encrypt if requested
	if b.encrypted {
		var err error
		data, err = seal(b.key, hash, data)
		if err != nil {
			return err
		}
		// Re-compute the hash
		hash = hashutil.Compute(data)
	}

	// Prepare the upload request
	params := &s3.PutObjectInput{
		Bucket:   aws.String(b.bucket),
		Key:      aws.String(hash),
		Body:     bytes.NewReader(data),
		Metadata: map[string]*string{},
	}

	// Actually upload the blob
	if _, err := b.s3.PutObject(params); err != nil {
		return err
	}

	// Save the hash in the local index
	if err := b.index.Index(hash); err != nil {
		return nil
	}

	return nil
}

func (b *S3Backend) Exists(hash string) (bool, error) {
	return false, ErrWriteOnly
}

func (b *S3Backend) Get(hash string) (data []byte, err error) {
	return nil, ErrWriteOnly
}

func (b *S3Backend) Close() {
	b.stop <- struct{}{}
	b.wg.Wait()
	b.queue.Close()
	b.index.Close()
}
