package docstore // import "a4.io/blobstash/pkg/client/docstore"

import (
	"bytes"
	"context"
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
	client *clientutil.ClientUtil
}

// Collection represents a collection of documents
type Collection struct {
	docstore *DocStore
	col      string
}

// InsertOpts defines the options for the `Insert` operation
// TODO(tsileo): use the same opts style as clientutil (...options)
type InsertOpts struct {
	Indexed bool
}

// DefaultInsertOpts initializes a new `InsertOpts` with sane default
func DefaultInsertOpts() *InsertOpts {
	return &InsertOpts{
		Indexed: false,
	}
}

// serverAddr should't have a trailing space
func New(client *clientutil.ClientUtil) *DocStore {
	return &DocStore{
		client: client,
	}
}

func (docstore *DocStore) Col(collection string) *Collection {
	return &Collection{
		docstore: docstore,
		col:      collection,
	}
}

func (col *Collection) Insert(ctx context.Context, idoc interface{}, opts *InsertOpts) (*ID, error) {
	//if opts == nil {
	//	opts = DefaultInsertOpts()
	//}
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
	// if opts.Indexed {
	// 	headers["BlobStash-DocStore-IndexFullText"] = "1"
	// }

	resp, err := col.docstore.client.Do("POST", fmt.Sprintf("/api/docstore/%s", col.col), payload)
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
func (col *Collection) UpdateID(ctx context.Context, id string, doc interface{}) error {
	js, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	resp, err := col.docstore.client.Do("POST", fmt.Sprintf("/api/docstore/%s/%s", col.col, id), bytes.NewReader(js))
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
func (col *Collection) GetID(ctx context.Context, id string, doc interface{}) error {
	resp, err := col.docstore.client.Get(fmt.Sprintf("/api/docstore/%s/%s", col.col, id))
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if err := clientutil.ExpectStatusCode(resp, 200); err != nil {
		if err.IsNotFound() {
			return ErrIDNotFound
		}
		return err
	}

	if err := clientutil.Unmarshal(resp, doc); err != nil {
		return err
	}

	return nil
}

type Iter struct {
	col   *Collection
	query *Query

	// FIXME(tsileo): attch the cursor?
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
	resp, err := iter.col.docstore.client.Get(u)
	if err != nil {
		iter.err = err
		return false
	}
	defer resp.Body.Close()
	if err := clientutil.ExpectStatusCode(resp, 200); err != nil {
		iter.err = err
		return false
	}

	if err := clientutil.Unmarshal(resp, res); err != nil {
		iter.err = err
		return false
	}

	iter.cursor = resp.Header.Get("BlobStash-DocStore-Iter-Cursor")
	hasMore, _ := strconv.ParseBool(resp.Header.Get("BlobStash-DocStore-Iter-Has-More"))
	if !hasMore {
		iter.closed = true // Next call will return false
	}
	return true
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
	resp, err := docstore.client.Get("/api/filetree/file/" + ref)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := clientutil.ExpectStatusCode(resp, 200); err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
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
	resp, err := docstore.client.Do("POST", "/api/filetree/upload?data="+url.QueryEscape(string(jsData)), bodyBuf, clientutil.WithHeader("Content-Type", contentType))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, 200); err != nil {
		return "", err
	}
	res := map[string]interface{}{}
	if err := clientutil.Unmarshal(resp, &res); err != nil {
		return "", err
	}
	return res["ref"].(string), nil
}

func (docstore *DocStore) Collections() ([]string, error) {
	resp, err := docstore.client.Get("/api/docstore/")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, 200); err != nil {
		return nil, err
	}

	colResp := &collectionResp{}
	if err := clientutil.Unmarshal(resp, colResp); err != nil {
		return nil, err
	}
	return colResp.Collections, nil
}
