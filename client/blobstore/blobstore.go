package blobstore

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"strconv"

	"github.com/tsileo/blobstash/client/ctx"
)

// ErrBlobNotFound is returned from a get/stat request
// if the blob does not exist.
var ErrBlobNotFound = errors.New("blob not found")

var defaultServerAddr = "http://localhost:9736"

type BlobStore struct {
	ServerAddr string
	client     *http.Client
}

func New(serverAddr string) *BlobStore {
	if serverAddr == "" {
		serverAddr = defaultServerAddr
	}
	return &BlobStore{
		ServerAddr: serverAddr,
		client:     &http.Client{},
	}
}

// Get fetch the given blob.
func (bs *BlobStore) Get(ctx *ctx.Ctx, hash string) ([]byte, error) {
	request, err := http.NewRequest("GET", bs.ServerAddr+"/blob/"+hash, nil)
	if err != nil {
		return nil, err
	}
	// TODO is this header really useful ?
	request.Header.Add("BlobStash-Namespace", ctx.Namespace)
	resp, err := bs.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == 200:
		return body, nil
	case resp.StatusCode == 404:
		return nil, ErrBlobNotFound
	default:
		return nil, fmt.Errorf("failed to put blob %v: %v", hash, string(body))
	}
}

// Stat checks wether a blob exists or not.
func (bs *BlobStore) Stat(ctx *ctx.Ctx, hash string) (bool, error) {
	request, err := http.NewRequest("HEAD", bs.ServerAddr+"/blob/"+hash, nil)
	if err != nil {
		return false, err
	}
	request.Header.Add("BlobStash-Namespace", ctx.Namespace)
	resp, err := bs.client.Do(request)
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		return true, nil
	case resp.StatusCode == 404:
		return false, nil
	default:
		return false, fmt.Errorf("failed to put blob %v", hash)
	}
}

// Put upload the given blob, the caller is responsible for computing the SHA-1 hash
func (bs *BlobStore) Put(ctx *ctx.Ctx, hash string, blob []byte) error {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(hash, hash)
	if err != nil {
		return err
	}
	if _, err := part.Write(blob); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	request, err := http.NewRequest("POST", bs.ServerAddr+"/upload", body)
	if err != nil {
		return err
	}
	request.Header.Add("BlobStash-Namespace", ctx.Namespace)
	request.Header.Add("BlobStash-Meta", strconv.FormatBool(ctx.MetaBlob))
	request.Header.Add("Content-Type", writer.FormDataContentType())
	resp, err := bs.client.Do(request)
	if err != nil {
		return err
	}
	body.Reset()
	body.ReadFrom(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to put blob %v", body.String())
	}
	return nil
}
