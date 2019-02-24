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

func IsVideo(filename string) bool {
	lname := strings.ToLower(filename)
	if strings.HasSuffix(lname, ".avi") {
		return true
	}
	return false
}

type Video struct {
	Width    int    `json:"width,omitempty" msgpack:"width,omitempty"`
	Height   int    `json:"height,omitempty" msgpack:"height,omitempty"`
	Codec    string `json:"codec,omitempty" msgpack:"codec,omitempty"`
	Duration int    `json:"duration,omitempty" msgpack:"duration,omitempty"`
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

func ThumbnailPath(conf *config.Config, hash string) string {
	return filepath.Join(conf.VidDir(), fmt.Sprintf("%s.jpg", hash))
}

func WebmPath(conf *config.Config, hash string) string {
	return filepath.Join(conf.VidDir(), fmt.Sprintf("%s.webm", hash))
}

func InfoPath(conf *config.Config, hash string) string {
	return filepath.Join(conf.VidDir(), fmt.Sprintf("%s.json", hash))
}

func buildThumbnail(conf *config.Config, p, hash string, duration int) error {
	rp := ThumbnailPath(conf, hash)
	//sec := math.Max(float64(duration), 59.0) / 2
	// FIXME(tsileo): compute a random screenshot ss
	cmd := exec.Command("ffmpeg", "-ss", fmt.Sprintf("00:00:12"), "-i", p, "-vframes", "1", "-vf", "scale='w=if(gt(a,16/9),854,-2):h=if(gt(a,16/9),-2,480)'", "-q:v", "2", rp)
	fmt.Printf("CMD=%+v\n", cmd)
	if dat, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %v", dat, err)
	}
	return nil
}

func buildWebm(conf *config.Config, p, hash string) error {
	webmPath := WebmPath(conf, hash)
	cmd := exec.Command("ffmpeg", "-i", p, "-vcodec", "libvpx", "-acodec", "libvorbis", "-vf", "scale='w=if(gt(a,16/9),854,-2):h=if(gt(a,16/9),-2,480)'", webmPath)
	fmt.Printf("CMD=%+v\n", cmd)
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

func Cache(conf *config.Config, p, hash string, duration int) error {
	if err := buildThumbnail(conf, p, hash, duration); err != nil {
		return err
	}
	if err := buildWebm(conf, p, hash); err != nil {
		return err
	}
	return nil
}

// Parse parses/probes a video file and returns the metadata (ffprobe required)
func Parse(p string) (*Video, error) {
	c := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,codec_name,codec_long_name:format=duration", "-of", "json", p)
	fmt.Printf("CMD=%+v\n", c)
	js, err := c.Output()
	if err != nil {
		return nil, fmt.Errorf("%s: %s", js, err)
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
