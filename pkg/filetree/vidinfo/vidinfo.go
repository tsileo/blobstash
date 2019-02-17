package vidinfo // import "a4.io/blobstash/pkg/filetree/vidinfo"

import (
	"time"

	"github.com/pixelbender/go-matroska/matroska"
)

type Video struct {
	Width    int `json:"width,omitempty"`
	Height   int `json:"height,omitempty"`
	Duration int `json:"duration"`
}

func Parse(p string) (*Video, error) {
	doc, err := matroska.Decode(p)
	if err != nil {
		return nil, err
	}
	w := int(doc.Segment.Tracks[0].Entries[0].Video.Width)
	h := int(doc.Segment.Tracks[0].Entries[0].Video.Height)
	d := int((time.Duration(doc.Segment.Info[0].Duration) * doc.Segment.Info[0].TimecodeScale).Seconds())

	return &Video{Width: w, Height: h, Duration: d}, nil
}
