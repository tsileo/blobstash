package clientutil // import "a4.io/blobstash/pkg/client/clientutil"

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

var ErrBlobNotFound = errors.New("blob not found")
var ErrNotFound = errors.New("not found")

var transport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	Dial: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
	TLSHandshakeTimeout: 10 * time.Second,
}
var setupHTTP2Once sync.Once

func setupHTTP2() {
	if err := http2.ConfigureTransport(transport.(*http.Transport)); err != nil {
		// TODO(tsileo): add a enable HTTP 2 flag in opts?
		fmt.Printf("HTTP2 ERROR: %+v", err)
	}

}

// Opts holds the client configuration
type Opts struct {
	Host   string // BlobStash host (with proto and without trailing slash) e.g. "https://blobtash.com"
	APIKey string // BlobStash API key

	Namespace string // BlobStash namespace

	Headers   map[string]string // Headers added to each request
	UserAgent string            // Custom User-Agent

	SnappyCompression bool // Enable snappy compression for the HTTP requests
	EnableHTTP2       bool // Enable HTTP2 as the client level
}

// SetNamespace is a shortcut for setting the namespace at the client level
func (opts *Opts) SetNamespace(ns string) *Opts {
	opts.Namespace = ns
	return opts
}

// SetHost is a configuration shortcut for setting the API hostname and the API key
func (opts *Opts) SetHost(host, apiKey string) *Opts {
	if host != "" {
		opts.Host = host
	}
	if apiKey != "" {
		opts.APIKey = apiKey
	}
	return opts
}

type Client struct {
	opts   *Opts
	client *http.Client
}

// New initializes an HTTP client
func New(opts *Opts) *Client {
	if opts == nil {
		panic("missing clientutil.Client opts")
	}
	if opts.EnableHTTP2 && strings.HasPrefix(opts.Host, "https") {
		setupHTTP2Once.Do(setupHTTP2)
	}
	client := &http.Client{
		Transport: transport,
	}
	return &Client{
		client: client,
		opts:   opts,
	}
}

// Opts returns the current opts
func (client *Client) Opts() *Opts {
	return client.opts
}

// DoReq "do" the request and returns the `*http.Response`
func (client *Client) DoReq(method, path string, headers map[string]string, body io.Reader) (*http.Response, error) {
	request, err := http.NewRequest(method, fmt.Sprintf("%s%s", client.opts.Host, path), body)
	if err != nil {
		return nil, err
	}

	if client.opts.APIKey != "" {
		request.SetBasicAuth("", client.opts.APIKey)
	}

	// Set our custom user agent
	if client.opts.UserAgent != "" {
		request.Header.Set("User-Agent", client.opts.UserAgent)
	}

	if client.opts.Namespace != "" {
		request.Header.Set("BlobStash-Namespace", client.opts.Namespace)
	}

	// Check if we should request compressed data
	if client.opts.SnappyCompression {
		request.Header.Set("Accept-Encoding", "snappy")
	}

	// Add custom headers
	for header, val := range client.opts.Headers {
		request.Header.Set(header, val)
	}
	for header, val := range headers {
		request.Header.Set(header, val)
	}
	return client.client.Do(request)
}

func (client *Client) GetJSON(path string, headers map[string]string, out interface{}) error {
	resp, err := client.DoReq("GET", path, headers, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 404 {
			return ErrNotFound
		}
		return fmt.Errorf("API call failed with status %d: %s", resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("failed to unmarshal: %v", err)
	}
	return nil
}
