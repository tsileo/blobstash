package blobstore

import (
	"net/http"
	"io"
	"mime/multipart"
	"bytes"
	"fmt"
	"strconv"
	"errors"
	"io/ioutil"
)

var (
	ErrBlobNotFound = errors.New("blob not found")
)

type Ctx struct {
  Namespace string
  MetaBlob bool
}

func GetBlob(ctx *Ctx, hash string) ([]byte, error) {
  request, err := http.NewRequest("GET", "http://0.0.0.0:9736/blob/"+hash, nil)
  if err != nil {
  	return nil, err
  }
  request.Header.Add("BlobStash-Namespace", ctx.Namespace)
  client := &http.Client{}
  resp, err := client.Do(request)
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
  		return nil, fmt.Errorf("failed to put blob %v: %v", hash, body)
  }
}

func StatBlob(ctx *Ctx, hash string) (bool, error) {
  request, err := http.NewRequest("HEAD", "http://0.0.0.0:9736/blob/"+hash, nil)
  if err != nil {
  	return false, err
  }
  request.Header.Add("BlobStash-Namespace", ctx.Namespace)
  client := &http.Client{}
  resp, err := client.Do(request)
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

func PutBlob(ctx *Ctx, hash string, blob []byte) error {
  body := &bytes.Buffer{}
  blobBuf := &bytes.Buffer{}
  blobBuf.Write(blob)
  writer := multipart.NewWriter(body)
  part, err := writer.CreateFormFile(hash, hash)
  if err != nil {
      return err
  }
  _, err = io.Copy(part, blobBuf)
  err = writer.Close()
  if err != nil {
      return err
  }
  request, err := http.NewRequest("POST", "http://0.0.0.0:9736/upload", body)
  if err != nil {
  	return err
  }
  // TODO put Ctx here
  request.Header.Add("BlobStash-Namespace", ctx.Namespace)
  request.Header.Add("BlobStash-Meta", strconv.FormatBool(ctx.MetaBlob))
  request.Header.Add("Content-Type", writer.FormDataContentType())
  client := &http.Client{}
  resp, err := client.Do(request)
  if err != nil {
  	return err
  }
  	body = &bytes.Buffer{}
    body.ReadFrom(resp.Body)
    resp.Body.Close()
    if resp.StatusCode != 200 {
    	return fmt.Errorf("failed to put blob %v", body)
    }
    return nil
}
