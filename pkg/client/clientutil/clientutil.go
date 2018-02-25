package clientutil // import "a4.io/blobstash/pkg/client/clientutil"

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/vmihailenco/msgpack"
)

var ErrBlobNotFound = errors.New("blob not found")
var ErrNotFound = errors.New("not found")

// FIXME(tsileo): more sensible default?
var transport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	Dial: (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
	TLSHandshakeTimeout: 5 * time.Second,
}

// Opts holds the client configuration
type Opts struct {
	Host   string // BlobStash host (with proto and without trailing slash) e.g. "https://blobtash.com"
	APIKey string // BlobStash API key

	Namespace string // BlobStash namespace

	Headers   map[string]string // Headers added to each request
	UserAgent string            // Custom User-Agent

	SnappyCompression bool // Enable snappy compression for the HTTP requests
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

	sessionID string
	mu        sync.Mutex // mutex for keeping the sessionID safe
}

// New initializes an HTTP client
func New(opts *Opts) *Client {
	if opts == nil {
		panic("missing clientutil.Client opts")
	}
	client := &http.Client{
		Transport: transport,
	}
	return &Client{
		client: client,
		opts:   opts,
	}
}

// ClientID returns a unique "session ID" that won't change for the lifetime of the client
func (client *Client) SessionID() string {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.sessionID != "" {
		return client.sessionID
	}
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	return hex.EncodeToString(data)
}

// Opts returns the current opts
func (client *Client) Opts() *Opts {
	return client.opts
}

// DoReq "do" the request and returns the `*http.Response`
func (client *Client) DoReq(ctx context.Context, method, path string, headers map[string]string, body io.Reader) (*http.Response, error) {
	request, err := http.NewRequest(method, fmt.Sprintf("%s%s", client.opts.Host, path), body)
	if err != nil {
		return nil, err
	}

	request = request.WithContext(ctx)

	request.Header.Set("BlobStash-Session-ID", client.SessionID())
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

func WithHeaders(headers map[string]string) func(*http.Request) error {
	return func(request *http.Request) error {
		for header, val := range headers {
			request.Header.Set(header, val)
		}
		return nil
	}
}

func WithQueryArgs(query map[string]string) func(*http.Request) error {
	return func(request *http.Request) error {
		q := request.URL.Query()
		for k, v := range query {
			q.Set(k, v)
		}
		request.URL.RawQuery = q.Encode()
		return nil
	}
}

func WithQueryArg(name, value string) func(*http.Request) error {
	return func(request *http.Request) error {
		q := request.URL.Query()
		q.Set(name, value)
		request.URL.RawQuery = q.Encode()
		return nil
	}
}

func WithAPIKey(apiKey string) func(*http.Request) error {
	return func(request *http.Request) error {
		request.SetBasicAuth("", apiKey)
		return nil
	}
}

func WithHeader(name, value string) func(*http.Request) error {
	return func(request *http.Request) error {
		request.Header.Set(name, value)
		return nil
	}
}

func WithUserAgent(ua string) func(*http.Request) error {
	return WithHeader("User-Agent", ua)
}

func EnableMsgpack() func(*http.Request) error {
	return WithHeader("Accept", "application/msgpack")
}

func EnableSnappyEncoding() func(*http.Request) error {
	return WithHeader("Accept-Encoding", "snappy")
}

type ClientUtil struct {
	host    string
	client  *http.Client
	options []func(*http.Request) error
}

// New initializes an HTTP client
func NewClientUtil(host string, options ...func(*http.Request) error) *ClientUtil {
	return &ClientUtil{
		host:    host,
		client:  http.DefaultClient,
		options: options,
	}
}

type BadStatusCodeError struct {
	Expected           int
	ResponseStatusCode int
	ResponseBody       []byte
	RequestMethod      string
	RequestURL         string

	// In case it failed before getting the response
	Err error
}

func (e *BadStatusCodeError) IsNotFound() (res bool) {
	if e.ResponseStatusCode == http.StatusNotFound {
		res = true
	}
	return
}

func (e *BadStatusCodeError) IsUnauthorized() (res bool) {
	if e.ResponseStatusCode == http.StatusUnauthorized {
		res = true
	}
	return
}

func (e *BadStatusCodeError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}

	return fmt.Sprintf("got status %d (expected %d) for %s request for %s: %s",
		e.ResponseStatusCode, e.Expected, e.RequestMethod, e.RequestURL, e.ResponseBody)
}

func ExpectStatusCode(resp *http.Response, status int) *BadStatusCodeError {
	if resp.StatusCode == status {
		return nil
	}

	// Not the expected status
	body, err := Decode(resp)
	if err != nil {
		return &BadStatusCodeError{Err: err}
	}

	return &BadStatusCodeError{
		Expected:           status,
		ResponseStatusCode: resp.StatusCode,
		ResponseBody:       body,
		RequestURL:         resp.Request.URL.String(),
		RequestMethod:      resp.Request.Method,
	}
}

func Decode(resp *http.Response) ([]byte, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// FIXME(tsileo): use a sync.Pool for the snappy reader (thanks to Reset on the snappy reader)
	if resp.Header.Get("Content-Encoding") == "snappy" {
		return snappy.Decode(nil, body)
	}

	return body, nil
}

func Unmarshal(resp *http.Response, out interface{}) error {
	body, err := Decode(resp)
	if err != nil {
		return err
	}

	contentType := resp.Header.Get("Content-Type")
	switch contentType {
	case "application/json":
		if err := json.Unmarshal(body, out); err != nil {
			return err
		}
		return nil
	case "application/msgpack":
		if err := msgpack.Unmarshal(body, out); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("unsupported \"%s\" content type", contentType)
}

func (client *ClientUtil) Delete(path string, options ...func(*http.Request) error) (*http.Response, error) {
	return client.Do("DELETE", path, nil, options...)
}

func (client *ClientUtil) Head(path string, options ...func(*http.Request) error) (*http.Response, error) {
	return client.Do("HEAD", path, nil, options...)
}

func (client *ClientUtil) Get(path string, options ...func(*http.Request) error) (*http.Response, error) {
	return client.Do("GET", path, nil, options...)
}

func (client *ClientUtil) Post(path string, data []byte, options ...func(*http.Request) error) (*http.Response, error) {
	body := bytes.NewReader(snappy.Encode(nil, data))
	options = append(options, WithHeader("Content-Type", "snappy"))

	return client.Do("POST", path, body, options...)
}

func (client *ClientUtil) doWithMsgpackBody(method, path string, payload interface{}, options ...func(*http.Request) error) (*http.Response, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := msgpack.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %v", err)
		}
		body = bytes.NewReader(encoded)
	}

	options = append(options, WithHeader("Content-Type", "application/msgpack"))
	return client.Do(method, path, body, options...)
}

func (client *ClientUtil) doWithJSONBody(method, path string, payload interface{}, options ...func(*http.Request) error) (*http.Response, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %v", err)
		}
		body = bytes.NewReader(encoded)
	}

	options = append(options, WithHeader("Content-Type", "application/json"))
	return client.Do(method, path, body, options...)
}

func (client *ClientUtil) PatchMsgpack(path string, payload interface{}, options ...func(*http.Request) error) (*http.Response, error) {
	return client.doWithMsgpackBody("PATCH", path, payload, options...)
}

func (client *ClientUtil) PostMsgpack(path string, payload interface{}, options ...func(*http.Request) error) (*http.Response, error) {
	return client.doWithMsgpackBody("POST", path, payload, options...)
}

func (client *ClientUtil) PostJSON(path string, payload interface{}, options ...func(*http.Request) error) (*http.Response, error) {
	return client.doWithJSONBody("POST", path, payload, options...)
}

func (client *ClientUtil) PatchJSON(path string, payload interface{}, options ...func(*http.Request) error) (*http.Response, error) {
	return client.doWithJSONBody("PATCH", path, payload, options...)
}

// DoReq "do" the request and returns the `*http.Response`
func (client *ClientUtil) Do(method, path string, body io.Reader, options ...func(*http.Request) error) (*http.Response, error) {
	// TODO(tsileo): a special/helper error for bad status code in Do that can return a BadStatusCodeError?
	request, err := http.NewRequest(method, fmt.Sprintf("%s%s", client.host, path), body)
	if err != nil {
		return nil, err
	}

	for _, option := range client.options {
		if err := option(request); err != nil {
			return nil, fmt.Errorf("failed to set client option %v: %v", option, err)
		}
	}

	for _, option := range options {
		if err := option(request); err != nil {
			return nil, fmt.Errorf("failed to set request option %v: %v", option, err)
		}
	}

	return client.client.Do(request)
}

func (client *ClientUtil) CheckAuth() (bool, error) {
	resp, err := client.Get("/api/ping")
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	serr := ExpectStatusCode(resp, http.StatusOK)

	if serr == nil {
		return true, nil
	}

	if serr.IsUnauthorized() {
		return false, nil
	}

	return false, serr
}

// DoReq "do" the request and returns the `*http.Response`
func (client *Client) DoReqWithQuery(ctx context.Context, method, path string, query map[string]string, headers map[string]string, body io.Reader) (*http.Response, error) {
	request, err := http.NewRequest(method, fmt.Sprintf("%s%s", client.opts.Host, path), body)
	if err != nil {
		return nil, err
	}

	q := request.URL.Query()
	for k, v := range query {
		q.Set(k, v)
	}
	request.URL.RawQuery = q.Encode()

	request = request.WithContext(ctx)

	request.Header.Set("BlobStash-Session-ID", client.SessionID())
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

func (client *Client) GetJSON(ctx context.Context, path string, headers map[string]string, out interface{}) error {
	resp, err := client.DoReq(ctx, "GET", path, headers, nil)
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
