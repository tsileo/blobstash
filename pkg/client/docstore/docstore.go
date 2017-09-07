package docstore // import "a4.io/blobstash/pkg/client/docstore"

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/url"
	"time"
	// "reflect"
	"strconv"

	"a4.io/blobstash/pkg/client/clientutil"
)

var ErrIDNotFound = errors.New("ID doest not exist")

var (
	defaultServerAddr = "http://localhost:8050"
	defaultUserAgent  = "DocStore Go client v1"
)

type ID struct {
	data []byte
	hash string
}

// Hash returns the hash of the JSON blob
func (id *ID) Hash() string {
	return id.hash
}

// ETag returns the ETag of the current document for future conditional requests
func (id *ID) ETag() string {
	return id.hash
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
	// if opts.Indexed {
	// 	headers["BlobStash-DocStore-IndexFullText"] = "1"
	// }
	resp, err := col.docstore.client.DoReq("POST", fmt.Sprintf("/api/docstore/%s", col.col), headers, payload)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 204, 200, 201:
		_id, err := IDFromHex(resp.Header.Get("BlobStash-DocStore-Doc-Id"))
		if err != nil {
			return nil, err
		}
		_id.hash = resp.Header.Get("BlobStash-DocStore-Doc-Hash")
		return _id, nil
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return nil, fmt.Errorf("failed to insert doc (%d): %v", resp.StatusCode, body.String())
	}
}

// Update the whole document
func (col *Collection) UpdateID(id string, doc interface{}) error {
	js, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	resp, err := col.docstore.client.DoReq("POST", fmt.Sprintf("/api/docstore/%s/%s", col.col, id), nil, bytes.NewReader(js))
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
	resp, err := col.docstore.client.DoReq("GET", fmt.Sprintf("/api/docstore/%s/%s", col.col, id), nil, nil)
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
	query *Query

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
	u := fmt.Sprintf("/api/docstore/%s?limit=%d", iter.col.col, iter.Opts.Limit)
	if iter.cursor != "" {
		u = u + "&cursor=" + iter.cursor
	}
	qqs := iter.query.ToQueryString()
	if qqs != "" {
		u = u + "&" + qqs
	}
	resp, err := iter.col.docstore.client.DoReq("GET", u, nil, nil)
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

type Query struct {
	StoredQuery     string
	StoredQueryArgs interface{}

	Query string

	Script string
}

func (q *Query) ToQueryString() string {
	if q.Query != "" {
		return fmt.Sprintf("query=%s", url.QueryEscape(q.Query))
	}
	if q.Script != "" {
		return fmt.Sprintf("script=%s", url.QueryEscape(q.Script))
	}
	if q.StoredQueryArgs != nil {
		js, err := json.Marshal(q.StoredQueryArgs)
		if err != nil {
			panic(err)
		}
		return fmt.Sprintf("stored_query=%s&stored_query_args=%s", q.StoredQuery, url.QueryEscape(string(js)))
	}
	return ""
}

func (col *Collection) Iter(query *Query, opts *IterOpts) (*Iter, error) {
	if opts == nil {
		opts = DefaultIterOtps()
	}
	if query == nil {
		query = &Query{}
	}
	return &Iter{
		col:   col,
		query: query,
		Opts:  opts,
	}, nil
}

type collectionResp struct {
	Collections []string `json:"collections"`
}

// DownloadAttachment returns an `io.ReadCloser` with the file content for the given ref.
func (docstore *DocStore) DownloadAttachment(ref string) (io.Reader, error) {
	resp, err := docstore.client.DoReq("GET", "/api/filetree/file/"+ref, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(data), nil
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return nil, fmt.Errorf("failed to insert doc: %v", body.String())
	}
}

func (docstore *DocStore) UploadAttachment(name string, r io.Reader, data map[string]interface{}) (string, error) {
	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)
	fileWriter, err := bodyWriter.CreateFormFile("file", name)
	if err != nil {
		return "", err
	}

	if _, err := io.Copy(fileWriter, r); err != nil {
		return "", err
	}

	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()
	// FIXME(tsileo):
	jsData, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	// FIXME(tsileo): make the server url.QueryUnescape the result and set it to data
	resp, err := docstore.client.DoReq("POST", "/api/filetree/upload?data="+url.QueryEscape(string(jsData)), map[string]string{
		"Content-Type": contentType,
	}, bodyBuf)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		res := map[string]interface{}{}
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			return "", err
		}
		return res["ref"].(string), nil
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return "", fmt.Errorf("failed to insert doc: %v", body.String())
	}
}

func (docstore *DocStore) Collections() ([]string, error) {
	resp, err := docstore.client.DoReq("GET", "/api/docstore/", nil, nil)
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
