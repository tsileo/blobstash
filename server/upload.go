/*
http://sanatgersappa.blogspot.fr/2013/03/handling-multiple-file-uploads-in-go.html

*/
package server

import (
	"net/http"
	"log"
	"bytes"
	"io"
	"strconv"

	"github.com/tsileo/blobstash/backend"
)

func uploadHandler(jobc chan<- *blobPutJob) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {

		//POST takes the uploaded file(s) and saves it to disk.
		case "POST":
			hostname := r.Header.Get("BlobStash-Hostname")
			ctx := r.Header.Get("BlobStash-Ctx")
			meta, _ := strconv.ParseBool(r.Header.Get("BlobStash-Meta"))
			log.Printf("Upload header: %v, %v, %v", hostname, ctx, meta)

			//parse the multipart form in the request
			mr, err := r.MultipartReader()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				hash := part.FormName()
				var buf bytes.Buffer
				buf.ReadFrom(part)
				breq := &backend.Request{
					Host: hostname,
					MetaBlob: meta,
					Archive: false,
				}
				jobc<- newBlobPutJob(breq, hash, buf.Bytes(), nil)
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
