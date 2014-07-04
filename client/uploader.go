package client

import (
	"net/http"
	"io"
	"mime/multipart"
	"bytes"
	"fmt"
	"strconv"
)

func StatBlob(hash string) (bool, error) {
  request, err := http.NewRequest("HEAD", "http://0.0.0.0:9736/blob/"+hash, nil)
  if err != nil {
  	return false, err
  }
  // TODO put Ctx here
  //request.Header.Add("BlobStash-Hostname", hostname)
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
  request.Header.Add("BlobStash-Hostname", ctx.Hostname)
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