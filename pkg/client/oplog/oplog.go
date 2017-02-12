package oplog // import "a4.io/blobstash/pkg/client/oplog"

import (
	"bufio"
	"bytes"
	"fmt"

	"a4.io/blobstash/pkg/client/clientutil"
)

var (
	headerEvent = []byte("event:")
	headerData  = []byte("data:")
)

// FIXME(tsileo): move this to client util
var defaultServerAddr = "http://localhost:8050"
var defaultUserAgent = "Oplog Go client v1"

type Oplog struct {
	client *clientutil.Client
}

type Op struct {
	Event string
	Data  string
}

// FIXME(tsileo): same here, move this to client util
func DefaultOpts() *clientutil.Opts {
	return &clientutil.Opts{
		Host:              defaultServerAddr,
		UserAgent:         defaultUserAgent,
		APIKey:            "",
		EnableHTTP2:       true,
		SnappyCompression: false,
	}
}

func New(opts *clientutil.Opts) *Oplog {
	if opts == nil {
		opts = DefaultOpts()
	}
	return &Oplog{
		client: clientutil.New(opts),
	}
}

func (o *Oplog) Notify(ops chan *Op) error {
	resp, err := o.client.DoReq("GET", "/_oplog/", nil, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	reader := bufio.NewReader(resp.Body)

	defer resp.Body.Close()
	// TODO(tsileo): a close channel?
	var op *Op
	for {
		// Read each new line and process the type of event
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.HasPrefix(line, headerEvent):
			if op == nil {
				op = &Op{}
			}
			// Remove header
			event := bytes.Replace(line, headerEvent, []byte(""), 1)
			op.Event = string(event[0 : len(event)-1]) // Remove newline
		case bytes.HasPrefix(line, headerData):
			if op == nil {
				op = &Op{}
			}
			// Remove header
			data := bytes.Replace(line, headerData, []byte(""), 1)
			op.Data = string(data[0 : len(data)-1]) // Remove newline
		default:
			if op != nil {
				ops <- op
				op = nil
			}
		}
	}

	return nil
}
