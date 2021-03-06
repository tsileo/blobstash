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

	resizer "github.com/nfnt/resize"

	"a4.io/blobstash/pkg/cache"
)

// Resize dynamically resizes an image
func Resize(cache *cache.Cache, hash, name string, f io.ReadSeeker, r *http.Request) (io.ReadSeeker, bool, error) {
	swi := r.URL.Query().Get("w")
	lname := strings.ToLower(name)
	if (strings.HasSuffix(lname, ".jpg") || strings.HasSuffix(lname, ".png") || strings.HasSuffix(lname, ".gif")) && swi != "" {
		wi, err := strconv.Atoi(swi)
		if err != nil {
			return nil, false, err
		}
		if cache != nil {
			data, ok, err := cache.Get(hash + swi)
			if err != nil {
				return nil, false, err
			}
			if ok {
				return bytes.NewReader(data), true, nil
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
			if err := cache.Add(hash+swi, b.Bytes()); err != nil {
				return nil, false, err
			}
		}
		return bytes.NewReader(b.Bytes()), true, nil
	}
	return f, false, nil
}
