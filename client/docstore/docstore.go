package docstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"github.com/tsileo/blobstash/client/clientutil"
)

var ErrIDNotFound = errors.New("ID doest not exist")

var defaultServerAddr = "http://localhost:8050"
var defaultUserAgent = "DocStore Go client v1"

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

func (col *Collection) Insert(idoc interface{}, opts *InsertOpts) error {
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
			return err
		}
		payload = bytes.NewReader(js)
	}
	headers := map[string]string{}
	if opts.Indexed {
		headers["BlobStash-DocStore-IndexFullText"] = "1"
	}
	resp, err := col.docstore.client.DoReq("POST", fmt.Sprintf("/api/ext/docstore/v1/%s", col.col), headers, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 204:
		// FIXME(tsileo): Set the _id field for a struct or add a key for the map
		return nil
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return fmt.Errorf("failed to insert doc: %v", body.String())
	}
}

func (col *Collection) UpdateID(id string, update map[string]interface{}) error {
	js, err := json.Marshal(update)
	if err != nil {
		return err
	}
	resp, err := col.docstore.client.DoReq("POST", fmt.Sprintf("/api/ext/docstore/v1/%s/%s", col.col, id), nil, bytes.NewReader(js))
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

	closed bool
	err    error
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
	resp, err := iter.col.docstore.client.DoReq("GET", fmt.Sprintf("/api/ext/docstore/v1/%s?query=%s", iter.col.col, iter.query), nil, nil)
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
		cnt, err := strconv.Atoi(resp.Header.Get("BlobStash-DocStore-Iter-Count"))
		if err != nil {
			iter.err = err
			return false
		}
		if cnt == iter.Opts.Limit {
			return true
		}
		iter.closed = true // Next call will return false
		// FIXME(tsileo): add the number of results (along with the latest ID / it will act as a cursor)
		// in a header and return true only if cnt == iter.Opts.Limit
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
