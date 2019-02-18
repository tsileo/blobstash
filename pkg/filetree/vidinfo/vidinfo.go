package vidinfo // import "a4.io/blobstash/pkg/filetree/vidinfo"

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"a4.io/blobstash/pkg/config"
)

type Video struct {
	Width    int    `json:"width,omitempty"`
	Height   int    `json:"height,omitempty"`
	Codec    string `json:"codec,omitempty"`
	Duration int    `json:"duration"`
}

type ffprobeResult struct {
	Streams []struct {
		CodecName     string `json:"codec_name"`
		CodecLongName string `json:"codec_long_name"`
		Width         int    `json:"width"`
		Height        int    `json:"height"`
	} `json:"streams"`
	Format struct {
		Duration string `json:"duration"`
	}
}

func buildThumbnail(conf *config.Config, p, hash string) error {
	webmPath := filepath.Join(conf.VarDir(), "webm", fmt.Sprintf("%s.jpg", hash))
	cmd := exec.Command("ffmpeg", "-ss", "00:00:15", "-i", p, "-vframes", "1", "-vf", "scale=\"'w=if(gt(a,16/9),854,-2):h=if(gt(a,16/9),-2,480)'\"", "-q:v", "2", webmPath)
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

func buildWebm(conf *config.Config, p, hash string) error {
	webmPath := filepath.Join(conf.VarDir(), "webm", fmt.Sprintf("%s.webm", hash))
	cmd := exec.Command("ffmpeg", "-i", p, "-vcodec", "libvpx", "-acodec", "libvorbis", "-vf", "scale=\"'w=if(gt(a,16/9),854,-2):h=if(gt(a,16/9),-2,480)'\"", webmPath)
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

// Parse parses/probes a video file and returns the metadata (ffprobe required)
func Parse(p string) (*Video, error) {
	js, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,codec_name,codec_long_name:format=duration", "-of", "json", p).Output()
	if err != nil {
		return nil, err
	}
	r := &ffprobeResult{}
	if err := json.Unmarshal(js, r); err != nil {
		return nil, err
	}
	d, err := strconv.Atoi(strings.Split(r.Format.Duration, ".")[0])
	if err != nil {
		return nil, err
	}

	return &Video{
		Width:    r.Streams[0].Width,
		Height:   r.Streams[0].Height,
		Codec:    r.Streams[0].CodecName,
		Duration: d,
	}, nil
}
