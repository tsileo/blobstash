package imginfo // import "a4.io/blobstash/pkg/filetree/imginfo"

import (
	// "encoding/json"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rwcarlsen/goexif/exif"
)

func IsImage(filename string) bool {
	lname := strings.ToLower(filename)
	if strings.HasSuffix(lname, ".jpg") {
		return true
	}
	return false
}

func getWidthHeight(f io.Reader) (int, int, error) {
	image, _, err := image.DecodeConfig(f)
	if err != nil {
		return 0, 0, err
	}
	return image.Width, image.Height, nil
}

type Image struct {
	Width  int       `json:"width,omitempty"`
	Height int       `json:"height,omitempty"`
	Exif   *ExifInfo `json:"exif,omitempty"`
}

type ExifInfo struct {
	Datetime  string  `json:"datetime,omitempty"`
	Make      string  `json:"make,omitempty"`
	Model     string  `json:"model,omitempty"`
	LensModel string  `json:"lens_model,omitempty"`
	GPSLat    float64 `json:"gps_lat,omitempty"`
	GPSLng    float64 `json:"gps_lng,omitempty"`
}

func parseExif(f io.Reader) (*ExifInfo, error) {
	x, err := exif.Decode(f)
	if err != nil {
		return nil, err
	}

	info := &ExifInfo{}
	camModel, err := x.Get(exif.Model)
	switch {
	case err == nil:
		camModelString, err := camModel.StringVal()
		if err != nil {
			return nil, err
		}
		info.Model = camModelString
	default:
		if _, ok := err.(exif.TagNotPresentError); !ok {
			return nil, err
		}
	}

	camMake, err := x.Get(exif.Make)
	switch {
	case err == nil:
		camMakeString, err := camMake.StringVal()
		if err != nil {
			return nil, err
		}
		info.Make = camMakeString
	default:
		if _, ok := err.(exif.TagNotPresentError); !ok {
			return nil, err
		}
	}

	lensModel, err := x.Get(exif.LensModel)
	switch {
	case err == nil:
		lensModelString, err := lensModel.StringVal()
		if err != nil {
			return nil, err
		}
		info.LensModel = lensModelString
	default:
		if _, ok := err.(exif.TagNotPresentError); !ok {
			return nil, err
		}
	}

	dt, err := x.DateTime()
	switch {
	case err == nil:
		info.Datetime = dt.Format(time.RFC3339)
	default:
		if _, ok := err.(exif.TagNotPresentError); !ok {
			return nil, err
		}
	}

	lat, long, err := x.LatLong()
	switch {
	case err == nil:
		info.GPSLat = lat
		info.GPSLng = long
	default:
		if _, ok := err.(exif.TagNotPresentError); !ok {
			return nil, err
		}
	}

	return info, nil
}

func Parse(f io.ReadSeeker, shouldParseExif bool) (*Image, error) {
	img := &Image{}
	w, h, err := getWidthHeight(f)
	if err == nil {
		img.Width = w
		img.Height = h
	}
	f.Seek(0, os.SEEK_SET)
	if shouldParseExif {
		ex, err := parseExif(f)
		if err == nil {
			img.Exif = ex
		}
	}
	return img, nil
}
