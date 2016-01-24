package docstore

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"
	// "reflect"
	"strconv"

	"github.com/tsileo/blobstash/client/clientutil"
)

var ErrIDNotFound = errors.New("ID doest not exist")

var defaultServerAddr = "http://localhost:8050"
var defaultUserAgent = "DocStore Go client v1"

type ID struct {
	data []byte
}

// String implements Stringer interface
func (id *ID) String() string {
	return hex.EncodeToString(id.data)
}

// Ts returns the timestamp component
func (id *ID) Time() time.Time {
	return time.Unix(int64(binary.BigEndian.Uint32(id.data[0:4])), 0)
}

// FromHex build an `ID` from an hex encoded string
func IDFromHex(data string) (*ID, error) {
	if len(data) != 24 {
		return nil, fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	b, err := hex.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	return &ID{data: b}, err
}

// Q is a wrapper of `map[string]interface{} for syntactic sugar
type Q map[string]interface{}
type M map[string]interface{}

type DocStore struct {
	client *clientutil.Client
}

// Collection represents a collection of documents
type Collection struct {
	docstore *DocStore
	col      string
}

// InsertOpts defines the options for the `Insert` operation
type InsertOpts struct {
	Indexed bool
}

// DefaultInsertOpts initializes a new `InsertOpts` with sane default
func DefaultInsertOpts() *InsertOpts {
	return &InsertOpts{
		Indexed: false,
	}
}

func DefaultOpts() *clientutil.Opts {
	return &clientutil.Opts{
		SnappyCompression: true,
		Host:              defaultServerAddr,
		UserAgent:         defaultUserAgent,
		APIKey:            "",
		EnableHTTP2:       true,
	}
}

// serverAddr should't have a trailing space
func New(opts *clientutil.Opts) *DocStore {
	if opts == nil {
		opts = DefaultOpts()
	}
	return &DocStore{
		client: clientutil.New(opts),
	}
}

func (docstore *DocStore) Col(collection string) *Collection {
	return &Collection{
		docstore: docstore,
		col:      collection,
	}
}

func (col *Collection) Insert(idoc interface{}, opts *InsertOpts) (*ID, error) {
	if opts == nil {
		opts = DefaultInsertOpts()
	}
	var js []byte
	var err error
	var payload io.Reader
	switch doc := idoc.(type) {
	case io.Reader:
		payload = doc
	case []byte:
		payload = bytes.NewReader(doc)
	default:
		if js, err = json.Marshal(doc); err != nil {
			return nil, err
		}
		payload = bytes.NewReader(js)
	}
	headers := map[string]string{}
	if opts.Indexed {
		headers["BlobStash-DocStore-IndexFullText"] = "1"
	}
	headers["BlobStash-Namespace"] = col.docstore.client.Opts().Namespace
	resp, err := col.docstore.client.DoReq("POST", fmt.Sprintf("/api/ext/docstore/v1/%s", col.col), headers, payload)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 204:
		return IDFromHex(resp.Header.Get("BlobStash-DocStore-Doc-Id"))
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return nil, fmt.Errorf("failed to insert doc: %v", body.String())
	}
}

func (col *Collection) UpdateID(id string, update map[string]interface{}) error {
	js, err := json.Marshal(update)
	if err != nil {
		return err
	}
	headers := map[string]string{
		"BlobStash-Namespace": col.docstore.client.Opts().Namespace,
	}
	resp, err := col.docstore.client.DoReq("POST", fmt.Sprintf("/api/ext/docstore/v1/%s/%s", col.col, id), headers, bytes.NewReader(js))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		return nil
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return fmt.Errorf("failed to insert doc: %v", body.String())
	}
}

// Get retrieve the document, `doc` must a map[string]interface{} or a struct pointer.
func (col *Collection) GetID(id string, doc interface{}) error {
	resp, err := col.docstore.client.DoReq("GET", fmt.Sprintf("/api/ext/docstore/v1/%s/%s", col.col, id), nil, nil)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		respBody := clientutil.NewSnappyResponseReader(resp)
		defer respBody.Close()
		if err := json.NewDecoder(respBody).Decode(doc); err != nil {
			return err
		}
		return nil
	case 404:
		return ErrIDNotFound
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return fmt.Errorf("failed to insert doc: %v", body.String())
	}
}

type Iter struct {
	col   *Collection
	query string // JSON marshalled and URI encoded query

	Opts     *IterOpts // Contains the current `IterOpts`
	LatestID string    // Needed for the subsequent API calls
	cursor   string

	closed bool
	err    error
}

func (iter *Iter) Cursor() string {
	return iter.cursor
}

func (iter *Iter) Close() error {
	iter.closed = true
	return iter.err
}

func (iter *Iter) Err() error {
	return iter.err
}

// Next unmarshall the request into the given slice,
// returns false when there's no more data
func (iter *Iter) Next(res interface{}) bool {
	if iter.closed {
		return false
	}
	resp, err := iter.col.docstore.client.DoReq("GET", fmt.Sprintf("/api/ext/docstore/v1/%s?cursor=%s&query=%s", iter.col.col, iter.cursor, iter.query), nil, nil)
	if err != nil {
		iter.err = err
		return false
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		respBody := clientutil.NewSnappyResponseReader(resp)
		defer respBody.Close()
		if err := json.NewDecoder(respBody).Decode(res); err != nil {
			iter.err = err
			return false
		}
		iter.cursor = resp.Header.Get("BlobStash-DocStore-Iter-Cursor")
		hasMore, _ := strconv.ParseBool(resp.Header.Get("BlobStash-DocStore-Iter-Has-More"))
		if !hasMore {
			iter.closed = true // Next call will return false
		}
		return true
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		iter.err = fmt.Errorf("failed to insert doc: %v", body.String())
		return false
	}
}

type IterOpts struct {
	Limit int
}

func DefaultIterOtps() *IterOpts {
	return &IterOpts{
		Limit: 50, // TODO(tsileo): tweak this
	}
}

func (col *Collection) Iter(query map[string]interface{}, opts *IterOpts) (*Iter, error) {
	if opts == nil {
		opts = DefaultIterOtps()
	}
	js, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	queryEscaped := url.QueryEscape(string(js))
	return &Iter{
		col:   col,
		query: queryEscaped,
		Opts:  opts,
	}, nil
}

type collectionResp struct {
	Collections []string `json:"collections"`
}

func (docstore *DocStore) Collections() ([]string, error) {
	resp, err := docstore.client.DoReq("GET", "/api/ext/docstore/v1/", nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		respBody := clientutil.NewSnappyResponseReader(resp)
		defer respBody.Close()
		colResp := &collectionResp{}
		if err := json.NewDecoder(respBody).Decode(colResp); err != nil {
			return nil, err
		}
		return colResp.Collections, nil
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return nil, fmt.Errorf("failed to insert doc: %v", body.String())
	}
}
