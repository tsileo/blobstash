package resize // import "a4.io/blobstash/pkg/httputil/resize"

import (
	"bytes"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/hashicorp/golang-lru"
	resizer "github.com/nfnt/resize"
)

// FIXME(tsileo): cache with thumbnails to speed-up, and ensure the browser can cache it? ETags aand Cache-Control

func Resize(cache *lru.Cache, hash, name string, f io.ReadSeeker, r *http.Request) (io.ReadSeeker, bool, error) {
	swi := r.URL.Query().Get("w")
	lname := strings.ToLower(name)
	if (strings.HasSuffix(lname, ".jpg") || strings.HasSuffix(lname, ".png") || strings.HasSuffix(lname, ".gif")) && swi != "" {
		wi, err := strconv.Atoi(swi)
		if err != nil {
			return nil, false, err
		}
		if cache != nil {
			if res, ok := cache.Get(hash + swi); ok {
				return bytes.NewReader(res.([]byte)), true, nil
			}
		}
		img, format, err := image.Decode(f)
		if err != nil {
			return nil, false, err
		}

		// resize to width `wi` using Lanczos resampling
		// and preserve aspect ratio
		m := resizer.Resize(uint(wi), 0, img, resizer.Lanczos3)
		b := &bytes.Buffer{}

		switch format {
		case "jpeg":
			if err := jpeg.Encode(b, m, nil); err != nil {
				return nil, false, err
			}
		case "gif":
			if err := gif.Encode(b, m, nil); err != nil {
				return nil, false, err
			}

		case "png":
			if err := png.Encode(b, m); err != nil {
				return nil, false, err
			}

		}
		if cache != nil {
			cache.Add(hash+swi, b.Bytes())
		}
		return bytes.NewReader(b.Bytes()), true, nil
	}
	return f, false, nil
}
