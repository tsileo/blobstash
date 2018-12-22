/*

Package client implements a server-sent event API client.

*/
package client // import "a4.io/ssse/pkg/client"
import (
	"bufio"
	"bytes"
	"errors"
	"net/http"
	"net/url"
)

// ErrInvalidSubscribeArgs is returned when a client tries to subscribe without a channel or a callback
var ErrInvalidSubscribeArgs = errors.New("must provides at leat a channel or a callback func in order to subscribe")

var (
	headerEvent = []byte("event:")
	headerData  = []byte("data:")
)

// SSEClient holds the client state
type SSEClient struct {
	url                string
	Username, Password string
}

// Event holds the event fields
type Event struct {
	Event string // Type of the event (i.e. "hearbeat", or you custom event type)
	Data  []byte // Data field
}

// New initialize a new client
func New(url string) *SSEClient {
	return &SSEClient{url: url}
}

// Subscribe connects to the server-sent event endpoint.
func (c *SSEClient) Subscribe(events chan<- *Event, callback func(*Event) error, filterEvents ...string) error {
	if events == nil && callback == nil {
		return ErrInvalidSubscribeArgs
	}

	vs := url.Values{}
	for _, evt := range filterEvents {
		vs.Add("event", evt)
	}

	req, err := http.NewRequest("GET", c.url+"?"+vs.Encode(), nil)
	if err != nil {
		return err
	}

	if c.Username != "" || c.Password != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Connection", "keep-alive")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)

	var event *Event
	for {
		// Read each new line and process the type of event
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.HasPrefix(line, headerEvent):
			if event == nil {
				event = &Event{}
			}
			// Remove header
			eventType := bytes.Replace(line, headerEvent, []byte(""), 1)
			event.Event = string(eventType[1 : len(eventType)-1]) // Remove initial space and newline
		case bytes.HasPrefix(line, headerData):
			if event == nil {
				event = &Event{}
			}
			// Remove header
			data := bytes.Replace(line, headerData, []byte(""), 1)
			event.Data = data[1 : len(data)-1] // Remove initial space and newline
		default:
			if event != nil && event.Event != "heartbeat" {
				if events != nil {
					events <- event
				} else {
					if err := callback(event); err != nil {
						return err
					}
				}
				event = nil
			}
		}
	}
}
