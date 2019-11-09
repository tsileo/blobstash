/*

http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-config-files


*/
package s3util // import "a4.io/blobstash/pkg/backend/s3/s3util"

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"a4.io/blobstash/pkg/blob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"golang.org/x/crypto/nacl/secretbox"
)

// The length of the nonce used for the secretbox implementation.
const nonceLength = 24

// The length of the encryption key for the secretbox implementation.
const keyLength = 32

// Set a flag to identify the encryption algorithm in case we support/switch encryption scheme later
const (
	naclSecretBox byte = 1 << iota
)

const (
	versionFlag byte = 1 << iota
)

var (
	blobHeader = []byte("#blobstash/secretbox\n")
)

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

func New(region string) (*session.Session, error) {
	return session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String(region),
		},
	})
}

func NewWithCustomEndoint(accessKey, secretKey, region, url string) (*session.Session, error) {
	defaultResolver := endpoints.DefaultResolver()
	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	s3CustResolverFn := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		if service == "s3" {
			return endpoints.ResolvedEndpoint{
				URL: url,
			}, nil
		}

		return defaultResolver.EndpointFor(service, region, optFns...)
	}
	return session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:           aws.String("us-east-1"),
			EndpointResolver: endpoints.ResolverFunc(s3CustResolverFn),
			Credentials:      creds,
		},
	})
}

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

func (b *Bucket) GetObject(key string) (*Object, error) {
	return &Object{
		s3:     b.s3,
		Bucket: b.Name,
		Key:    key,
	}, nil
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
		if strings.HasPrefix(*item.Key, "tmp/") {
			continue
		}
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

func (o *Object) Delete() error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(o.Bucket),
		Key:    aws.String(o.Key),
	}
	if _, err := o.s3.DeleteObject(params); err != nil {
		return err
	}
	return nil
}

func (o *Object) Copy(dest string) error {
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(o.Bucket),
		CopySource: aws.String(fmt.Sprintf("/%s/%s", o.Bucket, o.Key)),
		Key:        aws.String(dest),
	}
	if _, err := o.s3.CopyObject(input); err != nil {
		return err
	}
	return nil

}

func (o *Object) Exists() (bool, error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(o.Bucket),
		Key:    aws.String(o.Key),
	}
	_, err := o.s3.HeadObject(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return false, nil
		}
		return false, err
	}
	return true, nil
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
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(blobHeader, data[0:21]) {
		return nil, fmt.Errorf("missing header (\"%s\")", data[0:21])
	}

	decoded, err := Open(b.key, data)
	if err != nil {
		return nil, err
	}

	return decoded, nil
}

func (b *EncryptedBlob) HashAndPlainText() (string, []byte, error) {
	r, err := b.o.Reader()
	if err != nil {
		return "", nil, err
	}
	defer r.Close()
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return "", nil, err
	}

	if !bytes.Equal(blobHeader, data[0:21]) {
		return "", nil, fmt.Errorf("missing header (\"%s\")", data[0:21])
	}

	decoded, err := Open(b.key, data)
	if err != nil {
		return "", nil, err
	}

	return hex.EncodeToString(data[21:53]), decoded, nil
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
func Seal(nkey *[32]byte, blb *blob.Blob) ([]byte, error) {
	// FIXME(tsileo): store a flag for "data blobs"
	nonce := new([nonceLength]byte)
	if _, err := rand.Reader.Read(nonce[:]); err != nil {
		return nil, err
	}
	bhash, err := hex.DecodeString(blb.Hash)
	if err != nil {
		return nil, err
	}
	// Box will contains our meta data (alg byte + salt + nonce + flag)
	box := make([]byte, nonceLength+len(blobHeader)+len(bhash)+2)
	copy(box[:], blobHeader)
	copy(box[len(blobHeader):], bhash)
	// Add the version flag
	copy(box[len(blobHeader)+len(bhash):], []byte{versionFlag})

	// Add the "data blob" flag
	flag := []byte{0}
	if blb.IsFiletreeNode() || blb.IsMeta() {
		flag = []byte{1}
	}

	copy(box[len(blobHeader)+len(bhash)+1:], flag)
	// And the nonce
	copy(box[len(blobHeader)+len(bhash)+2:], nonce[:])
	return secretbox.Seal(box, blb.Data, nonce, nkey), nil
}

// Open a previously sealed secretbox with the key derived from `password` (using scrypt)
func Open(nkey *[32]byte, data []byte) ([]byte, error) {
	// Extract the nonce
	nonce := new([nonceLength]byte)
	copy(nonce[:], data[len(blobHeader)+32+2:len(blobHeader)+32+2+nonceLength])
	box := data[nonceLength+32+2+len(blobHeader):]
	// Actually decrypt the cipher text
	decrypted, success := secretbox.Open(nil, box, nonce, nkey)

	// Ensure the decryption succeed
	if !success {
		return nil, errors.New("failed to decrypt file (bad password?)")
	}

	return decrypted, nil
}
