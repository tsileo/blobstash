package filetree // import "a4.io/blobstash/pkg/filetree"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/hashicorp/golang-lru"
	log "github.com/inconshreveable/log15"
	"github.com/vmihailenco/msgpack"

	"a4.io/blobsfile"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/cache"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/ctxutil"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/imginfo"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
	"a4.io/blobstash/pkg/filetree/writer"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/httputil/bewit"
	"a4.io/blobstash/pkg/httputil/resize"
	"a4.io/blobstash/pkg/hub"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
)

var (
	// FIXME(tsileo): add a way to set a custom fmt key life for Blobs CLI as we don't care about the FS?
	indexFile = "index.html"
	FSKeyFmt  = "_filetree:fs:%s"

	MaxUploadSize int64 = 512 << 20 // 512MB
)

// FSUpdateEvent represents an even fired on FS update to the Oplog
type FSUpdateEvent struct {
	Name      string `json:"fs_name"`
	Path      string `json:"fs_path"`
	Ref       string `json:"node_ref"`
	Type      string `json:"node_type"`
	Time      int64  `json:"event_time"`
	Hostname  string `json:"event_hostname"`
	SessionID string `json:"session_id"`
}

func (e *FSUpdateEvent) JSON() string {
	js, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return string(js)
}

type FileTree struct {
	kvStore       store.KvStore
	blobStore     store.BlobStore
	conf          *config.Config
	hub           *hub.Hub
	sharingCred   *bewit.Cred
	authFunc      func(*http.Request) bool
	shareTTL      time.Duration
	thumbCache    *cache.Cache
	metadataCache *cache.Cache
	nodeCache     *lru.Cache

	log log.Logger
}

func (ft *FileTree) SharingCred() *bewit.Cred {
	return ft.sharingCred
}

func (ft *FileTree) ShareTTL() time.Duration {
	return ft.shareTTL
}

// BlobStore is the interface to be compatible with both the server and the BlobStore client
type BlobStore struct {
	blobStore store.BlobStore
	ctx       context.Context
}

func (bs *BlobStore) Get(hash string) ([]byte, error) {
	return bs.blobStore.Get(bs.ctx, hash)
}

func (bs *BlobStore) Stat(hash string) (bool, error) {
	return bs.blobStore.Stat(bs.ctx, hash)
}

func (bs *BlobStore) Put(ctx context.Context, hash string, data []byte) error {
	return bs.blobStore.Put(ctx, &blob.Blob{Hash: hash, Data: data})
}

// TODO(tsileo): a way to create a snapchat without modifying anything (and forcing the datactx before)
type Snapshot struct {
	Ref       string `msgpack:"-"`
	CreatedAt int64  `msgpack:"-"`

	Version  string `msgpack:"v"`
	Hostname string `msgpack:"h"`
	Message  string `msgpack:"m,omitempty"`
}

type FS struct {
	Name string `json:"-"`
	Ref  string `json:"ref"`

	ft *FileTree `json:"-"`
}

// New initializes the `DocStoreExt`
func New(logger log.Logger, conf *config.Config, authFunc func(*http.Request) bool, kvStore store.KvStore, blobStore store.BlobStore, chub *hub.Hub) (*FileTree, error) {
	logger.Debug("init")
	// FIXME(tsileo): make the number of thumbnails to keep in memory a config item
	thumbscache, err := cache.New(conf.VarDir(), "filetree_thumbs.cache", 512<<20)
	if err != nil {
		return nil, err
	}
	metacache, err := cache.New(conf.VarDir(), "filetree_info.cache", 256<<20)
	if err != nil {
		return nil, err
	}
	nodeCache, err := lru.New(512)
	if err != nil {
		return nil, err
	}

	return &FileTree{
		conf:      conf,
		kvStore:   kvStore,
		blobStore: blobStore,
		sharingCred: &bewit.Cred{
			Key: []byte(conf.SharingKey),
			ID:  "filetree",
		},
		thumbCache:    thumbscache,
		metadataCache: metacache,
		nodeCache:     nodeCache,
		authFunc:      authFunc,
		shareTTL:      1 * time.Hour,
		hub:           chub,
		log:           logger,
	}, nil
}

// Close closes all the open DB files.
func (ft *FileTree) Close() error {
	ft.thumbCache.Close()
	ft.metadataCache.Close()
	return nil
}

// RegisterRoute registers all the HTTP handlers for the extension
func (ft *FileTree) Register(r *mux.Router, root *mux.Router, basicAuth func(http.Handler) http.Handler) {
	// Raw node endpoint
	r.Handle("/node/{ref}", basicAuth(http.HandlerFunc(ft.nodeHandler())))

	// Public/semi-private handler
	dirHandler := http.HandlerFunc(ft.dirHandler())
	fileHandler := http.HandlerFunc(ft.fileHandler())

	r.Handle("/fs", basicAuth(http.HandlerFunc(ft.fsRootHandler())))
	r.Handle("/fs/{type}/{name}/", basicAuth(http.HandlerFunc(ft.fsHandler())))
	r.Handle("/fs/{type}/{name}/{path:.+}", basicAuth(http.HandlerFunc(ft.fsHandler())))
	// r.Handle("/fs", http.HandlerFunc(ft.fsHandler()))
	// r.Handle("/fs/{name}", http.HandlerFunc(ft.fsByNameHandler()))

	r.Handle("/upload", basicAuth(http.HandlerFunc(ft.uploadHandler())))

	// Hook the standard endpint
	r.Handle("/dir/{ref}", dirHandler)
	r.Handle("/file/{ref}", fileHandler)

	// r.Handle("/index/{ref}", basicAuth(http.HandlerFunc(ft.indexHandler())))

	// Enable shortcut path from the root
	root.Handle("/d/{ref}", dirHandler)
	root.Handle("/f/{ref}", fileHandler)
}

// Node holds the data about the file node (either file/dir), analog to a Meta
type Node struct {
	Name       string  `json:"name" msgpack:"n"`
	Type       string  `json:"type" msgpack:"t"`
	Size       int     `json:"size,omitempty" msgpack:"s,omitempty"`
	Mode       int     `json:"mode,omitempty" msgpack:"mo,omitempty"`
	ModTime    string  `json:"mtime" msgpack:"mt"`
	ChangeTime string  `json:"ctime" msgpack:"ct"`
	Hash       string  `json:"ref" msgpack:"r"`
	Children   []*Node `json:"children,omitempty" msgpack:"c,omitempty"`

	// FIXME(ts): rename to Metadata
	Data map[string]interface{} `json:"metadata,omitempty" msgpack:"md,omitempty"`
	Info *Info                  `json:"info,omitempty" msgpack:"i,omitempty"`

	Meta   *rnode.RawNode `json:"-" msgpack:"-"`
	parent *Node          `json:"-" msgpack:"-"`
	fs     *FS            `json:"-" msgpack:"-"`

	URL string `json:"url,omitempty" msgpack:"u,omitempty"`
}

// Update the given node with the given meta, the updated/new node is assumed to be already saved
func (ft *FileTree) Update(ctx context.Context, n *Node, m *rnode.RawNode, prefixFmt string, first bool) (*Node, error) {
	newNode, err := MetaToNode(m)
	if err != nil {
		return nil, err
	}
	newNode.fs = n.fs
	newNode.parent = n.parent
	// fmt.Printf("\n\n\n###Update: n=%+v\nn.meta=%+v\nn.parent=%+v\nm=%+v\nnewNode=%+v\n\n\n###", n, n.meta, n.parent, m, newNode)
	// if n.parent != nil {
	// 	fmt.Printf("n.parent.meta=%+v\n", n.parent.meta)

	// }
	if n.parent == nil {
		n.fs.Ref = newNode.Hash
		// js, err := json.Marshal(n.fs)
		// if err != nil {
		// return nil, err
		// }
		snap := &Snapshot{}
		if h, ok := ctxutil.FileTreeHostname(ctx); ok {
			snap.Hostname = h
		}
		snapEncoded, err := msgpack.Marshal(snap)
		if err != nil {
			return nil, err
		}
		if ft.kvStore.Put(ctx, fmt.Sprintf(prefixFmt, n.fs.Name), newNode.Hash, snapEncoded, -1); err != nil {
			return nil, err
		}
		return newNode, nil
	}

	newRefs := []interface{}{newNode.Hash}
	newChildren := []*Node{newNode}

	for _, c := range newNode.parent.Children {
		if c.Name != n.Name {
			newRefs = append(newRefs, c.Hash)
			newChildren = append(newChildren, c)
		}
	}

	newNode.parent.Meta.Refs = newRefs
	newNode.parent.Children = newChildren

	if first {
		newNode.parent.Meta.ModTime = m.ModTime
		newNode.parent.ModTime = time.Unix(m.ModTime, 0).Format(time.RFC3339)
	}
	if newNode.parent.Meta.ModTime == 0 {
		t := time.Now()
		newNode.parent.Meta.ModTime = t.Unix()
		newNode.parent.ModTime = t.Format(time.RFC3339)
	}
	// else {
	// 	if newNode.parent.Meta.ModTime == 0 { // || m.ModTime > newNode.parent.Meta.ModTime {
	// 		newNode.parent.Meta.ModTime = m.ModTime
	// 		newNode.parent.ModTime = time.Unix(m.ModTime, 0).Format(time.RFC3339)
	// 	}
	// }

	// parentMeta := n.parent.meta
	// parentMeta.Refs = newRefs
	// n.parent.Children = newChildren

	newRef, data := newNode.parent.Meta.Encode()
	newNode.parent.Hash = newRef
	newNode.parent.Meta.Hash = newRef
	if err := ft.blobStore.Put(ctx, &blob.Blob{Hash: newRef, Data: data}); err != nil {
		return nil, err
	}
	// n.parent.Hash = newRef
	// parentMeta.Hash = newRef
	// Propagate the change to the parents
	if _, err := ft.Update(ctx, newNode.parent, newNode.parent.Meta, prefixFmt, false); err != nil {
		return nil, err
	}
	return newNode, nil
}

// Update the given node with the given meta, the updated/new node is assumed to be already saved
func (ft *FileTree) AddChild(ctx context.Context, n *Node, newChild *rnode.RawNode, prefixFmt string, mtime int64) (*Node, error) {
	// Save the new child meta
	//newChild.ModTime = time.Now().UTC().Unix()
	newChildRef, data := newChild.Encode()
	newChild.Hash = newChildRef
	if err := ft.blobStore.Put(ctx, &blob.Blob{Hash: newChildRef, Data: data}); err != nil {
		return nil, err
	}
	newChildNode, err := MetaToNode(newChild)
	if err != nil {
		return nil, err
	}
	newChildNode.Hash = newChildRef

	// Add it as a new child for the node
	newRefs := []interface{}{newChildNode.Hash}
	newChildren := []*Node{newChildNode}

	var update bool
	for _, c := range n.Children {
		if c.Name == newChildNode.Name {
			update = true
			continue
		}
		newRefs = append(newRefs, c.Hash)
		newChildren = append(newChildren, c)
	}

	n.Meta.Refs = newRefs
	n.Children = newChildren

	// Update the parent dir mtime if it's not a file update (file was created or justed patched here)
	if !update && newChild.ModTime > 0 {
		n.Meta.ModTime = mtime
		n.Meta.ChangeTime = 0
	}

	// Save the new node (the updated dir)
	newRef, data := n.Meta.Encode()
	n.Hash = newRef
	n.Meta.Hash = newRef
	if err := ft.blobStore.Put(ctx, &blob.Blob{Hash: newRef, Data: data}); err != nil {
		return nil, err
	}

	// Proagate the change up to the ~moon~ root
	return ft.Update(ctx, n, n.Meta, prefixFmt, true)
}

// Delete removes the given node from its parent children
func (ft *FileTree) Delete(ctx context.Context, n *Node, prefixFmt string, mtime int64) (*Node, error) {
	if n.parent == nil {
		panic("can't delete root")
	}
	parent := n.parent

	newRefs := []interface{}{}
	newChildren := []*Node{}
	for _, c := range parent.Children {
		if c.Name != n.Name {
			newRefs = append(newRefs, c.Hash)
			newChildren = append(newChildren, c)
		}
	}

	parent.Meta.ModTime = mtime
	parent.Meta.ChangeTime = 0
	parent.Meta.Refs = newRefs
	parent.Children = newChildren
	newRef, data := parent.Meta.Encode()
	parent.Hash = newRef
	parent.Meta.Hash = newRef
	if err := ft.blobStore.Put(ctx, &blob.Blob{Hash: newRef, Data: data}); err != nil {
		return nil, err
	}

	return ft.Update(ctx, parent, parent.Meta, prefixFmt, true)
}

func (n *Node) Close() error {
	return nil
}

type byName []*Node

func (s byName) Len() int           { return len(s) }
func (s byName) Less(i, j int) bool { return s[i].Name < s[j].Name }
func (s byName) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func MetaToNode(m *rnode.RawNode) (*Node, error) {
	n := &Node{
		Name: m.Name,
		Type: m.Type,
		Size: m.Size,
		Data: m.Metadata,
		Hash: m.Hash,
		Mode: int(m.Mode),
		Meta: m,
	}
	if m.ModTime > 0 {
		n.ModTime = time.Unix(m.ModTime, 0).Format(time.RFC3339)
	}
	if m.ChangeTime > 0 {
		n.ChangeTime = time.Unix(m.ChangeTime, 0).Format(time.RFC3339)
	} else {
		// If there's no ctime, set the value to mtime
		if n.ModTime != "" {
			n.ChangeTime = n.ModTime
		}
	}
	return n, nil
}

// fetchDir recursively fetch dir children
func (ft *FileTree) fetchDir(ctx context.Context, n *Node, depth, maxDepth int) error {
	if depth > maxDepth {
		return nil
	}
	if n.Type == "dir" {
		n.Children = []*Node{}
		for _, ref := range n.Meta.Refs {
			cn, err := ft.nodeByRef(ctx, ref.(string))
			if err != nil {
				return err
			}
			n.Children = append(n.Children, cn)
			if err := ft.fetchDir(ctx, cn, depth+1, maxDepth); err != nil {
				return err
			}
		}
	}
	return nil
}

// FS fetch the FileSystem by name, returns an empty one if not found
func (ft *FileTree) FS(ctx context.Context, name, prefixFmt string, newState bool, asOf int) (*FS, error) {
	fs := &FS{}
	if !newState {
		if asOf == 0 {
			kv, err := ft.kvStore.Get(ctx, fmt.Sprintf(prefixFmt, name), -1)
			if err != nil && err != vkv.ErrNotFound {
				return nil, err
			}
			switch err {
			case nil:
				// Set the existing ref
				fs.Ref = kv.HexHash()
			case vkv.ErrNotFound:
				// XXX(tsileo): should the `ErrNotFound` be returned here?
			default:
				return nil, err
			}
		} else {
			// Set the existing ref
			kvv, _, err := ft.kvStore.Versions(ctx, fmt.Sprintf(prefixFmt, name), strconv.Itoa(asOf), 1)
			switch err {
			case nil:
				if len(kvv.Versions) > 0 {
					fs.Ref = kvv.Versions[0].HexHash()
				}

			case vkv.ErrNotFound:
				// XXX(tsileo): should the `ErrNotFound` be returned here?
			default:
				return nil, err
			}
		}
	}
	fs.Name = name
	fs.ft = ft
	return fs, nil
}

// Root fetch the FS root, and creates a new one if `create` is set to true (but it won't be savec automatically in the BlobStore
func (fs *FS) Root(ctx context.Context, create bool, mtime int64) (*Node, error) {
	fs.ft.log.Info("Root", "fs", fs)
	node, err := fs.ft.nodeByRef(ctx, fs.Ref)
	switch err {
	case blobsfile.ErrBlobNotFound:
		if !create {
			return nil, err
		}
		meta := &rnode.RawNode{
			Type:    "dir",
			Version: "1",
			Name:    "_root",
			ModTime: mtime,
		}
		node, err = MetaToNode(meta)
		if err != nil {
			return nil, err
		}
	case nil:
	default:
		return nil, err
	}
	node.fs = fs
	return node, nil
}

// Path returns the `Node` at the given path, create it if requested
func (fs *FS) Path(ctx context.Context, path string, create bool, mtime int64) (*Node, *rnode.RawNode, bool, error) {
	var found bool
	node, err := fs.Root(ctx, create, mtime)
	if err != nil {
		return nil, nil, found, err
	}
	var prev *Node
	var cmeta *rnode.RawNode
	node.fs = fs
	node.parent = nil
	if err := fs.ft.fetchDir(ctx, node, 1, 1); err != nil {
		return nil, nil, found, err
	}
	if path == "/" {
		fs.ft.log.Info("returning root")
		return node, cmeta, found, err
	}
	split := strings.Split(path[1:], "/")
	// fmt.Printf("split res=%+v\n", split)
	// Split the path, and fetch each node till the last one
	pathCount := len(split)
	for i, p := range split {
		prev = node
		found = false
		// fmt.Printf("split:%+v\n", p)
		for _, child := range node.Children {
			if child.Name == p {
				node, err = fs.ft.nodeByRef(ctx, child.Hash)
				if err != nil {
					return nil, nil, found, err
				}
				if err := fs.ft.fetchDir(ctx, node, 1, 1); err != nil {
					return nil, nil, found, err
				}
				node.parent = prev
				node.fs = fs
				// fmt.Printf("split:%+v fetched:%+v\n", p, node)
				found = true
				break
			}
		}
		// fmt.Printf("split:%+v, node=%+v\n", p, node)
		// At this point, we found no node at the given path
		if !found {
			if !create {
				return nil, nil, found, clientutil.ErrBlobNotFound
			}
			// Create a new dir since it doesn't exist
			cmeta = &rnode.RawNode{
				Type:    "dir",
				Version: "1",
				Name:    p,
				ModTime: mtime,
			}
			if i == pathCount-1 {
				cmeta.Type = "file"
			}
			//  we don't set the meta type, it will be set on Update if it doesn't exist
			node, err = MetaToNode(cmeta)
			if err != nil {
				return nil, nil, found, err
			}
			node.parent = prev
			node.fs = fs
			// fmt.Printf("split:%+v created:%+v\n", p, node)
		}
	}
	return node, cmeta, !found, nil
}
func (ft *FileTree) indexHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
		vars := mux.Vars(r)
		ref := vars["ref"]
		node, err := ft.nodeByRef(ctx, ref)
		if err != nil {
			if err == clientutil.ErrBlobNotFound {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			panic(err)
		}
		out := ft.buildIndex(ctx, "/", node)
		httputil.WriteJSON(w, out)
	}
}

func (ft *FileTree) buildIndex(ctx context.Context, path string, node *Node) map[string]string {
	out := map[string]string{}
	if err := ft.fetchDir(ctx, node, 1, 1); err != nil {
		panic(err)
	}
	dpath := filepath.Join(path, node.Name)
	for _, child := range node.Children {
		if child.Type == "file" {
			out[filepath.Join(dpath, child.Name)] = child.Hash
		} else {
			for p, ref := range ft.buildIndex(ctx, dpath, child) {
				out[p] = ref
			}
		}
	}
	if dpath != "/" {
		out[dpath+"/"] = node.Hash
	}
	return out
}

// Handle multipart form upload to create a new Node (outside of any FS)
func (ft *FileTree) uploadHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
		// Try to parse the metadata (JSON encoded in the `data` query argument)
		var data map[string]interface{}
		if d := r.URL.Query().Get("data"); d != "" {
			udata, err := url.QueryUnescape(d)
			if err != nil {
				panic(err)
			}
			if err := json.Unmarshal([]byte(udata), &data); err != nil {
				panic(err)
			}
		}
		fmt.Printf("parsed data=%+v\n", data)

		r.ParseMultipartForm(MaxUploadSize)
		file, handler, err := r.FormFile("file")
		if err != nil {
			panic(err)
		}
		defer file.Close()
		uploader := writer.NewUploader(&BlobStore{ft.blobStore, ctx})
		fdata, err := ioutil.ReadAll(file)
		if err != nil {
			panic(err)
		}
		reader := bytes.NewReader(fdata)
		meta, err := uploader.PutReader(handler.Filename, reader, data)
		if err != nil {
			panic(err)
		}
		reader.Seek(0, os.SEEK_SET)
		info, err := ft.fetchInfo(reader, handler.Filename, meta.Hash)
		if err != nil {
			panic(err)
		}
		node, err := MetaToNode(meta)
		if err != nil {
			panic(err)
		}
		node.Info = info
		httputil.WriteJSON(w, node)
	}
}

type Info struct {
	Image *imginfo.Image `json:"image",omitempty`
}

func (ft *FileTree) fetchInfo(reader io.ReadSeeker, filename, hash string) (*Info, error) {
	if ft.metadataCache != nil {
		cached, ok, err := ft.metadataCache.Get(hash)
		if err != nil {
			return nil, err
		}
		if ok {
			fmt.Printf("metadata from cache")
			info := &Info{}
			if err := json.Unmarshal(cached, info); err != nil {
				return nil, err
			}
			return info, nil
		}

	}

	info := &Info{}
	lname := strings.ToLower(filename)
	// TODO(tsileo): parse PDF text
	// XXX(tsileo): generate video thumbnail?
	if strings.HasSuffix(lname, ".jpg") || strings.HasSuffix(lname, ".png") || strings.HasSuffix(lname, ".gif") {
		var parseExif bool
		if strings.HasSuffix(lname, ".jpg") {
			parseExif = true
		}
		imageInfo, err := imginfo.Parse(reader, parseExif)
		if err == nil {
			info.Image = imageInfo
		}
	}

	if ft.metadataCache != nil {
		js, err := json.Marshal(info)
		if err != nil {
			return nil, err
		}
		if err := ft.metadataCache.Add(hash, js); err != nil {
			return nil, err
		}
	}

	return info, nil
}

// FIXME(ts): fix this one
func (ft *FileTree) fsRootHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))

		nodes := []*Node{}

		prefix := r.URL.Query().Get("prefix")
		if prefix != "" {
			prefix = prefix + ":%s"
		} else {
			prefix = FSKeyFmt
		}
		prefix = prefix[0 : len(prefix)-2]

		keys, _, err := ft.kvStore.Keys(ctx, prefix, prefix+"\xff", 0)
		if err != nil {
			panic(err)
		}
		// Put(context.TODO(), fmt.Sprintf(FSKeyFmt, n.fs.Name), "", js, -1); err != nil {

		for _, kv := range keys {
			data := strings.Split(kv.Key, ":")
			fs := &FS{Name: data[len(data)-1], Ref: kv.HexHash(), ft: ft}
			node, _, _, err := fs.Path(ctx, "/", false, 0)
			if err != nil {
				panic(err)
			}
			nodes = append(nodes, node)
		}
		httputil.WriteJSON(w, nodes)
	}
}

func fixPath(p string) string {
	if p == "." {
		return ""
	}
	return p
}

func (ft *FileTree) fsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := ctxutil.WithFileTreeHostname(r.Context(), r.Header.Get(ctxutil.FileTreeHostnameHeader))
		// FIXME(tsileo): handle mtime in the context too, and make it optional

		vars := mux.Vars(r)
		fsName := vars["name"]
		path := "/" + vars["path"]
		refType := vars["type"]
		prefixFmt := FSKeyFmt
		if p := r.URL.Query().Get("prefix"); p != "" {
			prefixFmt = p + ":%s"
		}
		var mtime int64
		var err error
		if st := r.URL.Query().Get("mtime"); st != "" {
			mtime, err = strconv.ParseInt(st, 10, 0)
			if err != nil {
				panic(err)
			}
		}
		var fs *FS
		switch refType {
		case "ref":
			fs = &FS{
				Ref: fsName,
				ft:  ft,
			}
		case "fs":
			fs, err = ft.FS(ctx, fsName, prefixFmt, false, 0)
			if err != nil {
				panic(err)
			}
		default:
			panic(fmt.Errorf("Unknown type \"%s\"", refType))
		}
		switch r.Method {
		case "GET", "HEAD":
			node, _, _, err := fs.Path(ctx, path, false, mtime)
			switch err {
			case nil:
			case clientutil.ErrBlobNotFound:
				// Returns a 404 if the blob/children is not found
				w.WriteHeader(http.StatusNotFound)
				return
			case blobsfile.ErrBlobNotFound:
				// Returns a 404 if the blob/children is not found
				w.WriteHeader(http.StatusNotFound)
				return
			default:
				panic(err)
			}

			w.Header().Set("ETag", node.Hash)

			// Handle HEAD request
			if r.Method == "HEAD" {
				return
			}

			// Returns the Node as JSON
			httputil.MarshalAndWrite(r, w, node)

		case "POST":
			// FIXME(tsileo): add a way to upload a file as public ? like AWS S3 public-read canned ACL
			// Add a new node in the FS at the given path
			node, _, created, err := fs.Path(ctx, path, true, mtime)
			if err != nil {
				panic(err)
			}

			if hash := r.Header.Get("If-Match"); hash != "" {
				if node.Hash != hash {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}

			// fmt.Printf("Current node:%v %+v %+v\n", path, node, node.meta)
			// fmt.Printf("Current node parent:%+v %+v\n", node.parent, node.parent.meta)
			r.ParseMultipartForm(MaxUploadSize)
			file, _, err := r.FormFile("file")
			if err != nil {
				panic(err)
			}
			defer file.Close()
			uploader := writer.NewUploader(&BlobStore{ft.blobStore, ctx})

			// Create/save me Meta
			meta, err := uploader.PutReader(filepath.Base(path), file, nil)
			if err != nil {
				panic(err)
			}
			meta.ModTime = mtime
			fmt.Printf("new meta=%+v\n", meta)

			// Update the Node with the new Meta
			// fmt.Printf("uploaded meta=%+v\nold node=%+v", meta, node)
			newNode, err := ft.Update(ctx, node, meta, prefixFmt, true)
			if err != nil {
				panic(err)
			}

			// Event handling for the oplog
			evtType := "file-updated"
			if created {
				evtType = "file-created"
			}
			updateEvent := &FSUpdateEvent{
				Name:      fs.Name,
				Type:      evtType,
				Ref:       newNode.Hash,
				Path:      path[1:],
				Time:      time.Now().UTC().Unix(),
				SessionID: httputil.GetSessionID(r),
			}
			if err := ft.hub.FiletreeFSUpdateEvent(ctx, nil, updateEvent.JSON()); err != nil {
				panic(err)
			}

			httputil.WriteJSON(w, newNode)

		case "PATCH":
			// Add a node (from its JSON representation) to a directory
			var err error
			// FIXME(tsileo): s/rename/change/ ? for the special ctime handling
			var rename bool
			if r := r.URL.Query().Get("rename"); r != "" {
				rename, err = strconv.ParseBool(r)
				if err != nil {
					panic(err)
				}
			}
			node, _, _, err := fs.Path(ctx, path, true, mtime)
			if err != nil {
				if err == blobsfile.ErrBlobNotFound {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				panic(err)
			}
			if node.Type != "dir" {
				panic("only dir can be patched")
			}

			if hash := r.Header.Get("If-Match"); hash != "" {
				if node.Hash != hash {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
			var newChild *rnode.RawNode

			if newRef := r.Header.Get("BlobStash-Filetree-Patch-Ref"); newRef != "" {

				blob, err := ft.blobStore.Get(ctx, newRef)
				if err != nil {
					panic(err)
				}

				newChild, err = rnode.NewNodeFromBlob(newRef, blob)
				if err != nil {
					panic(err)
				}

				if newChild == nil {
					// FIXME(tsileo): return a 404
					panic("cannot find node for patching")
				}

				if newName := r.Header.Get("BlobStash-Filetree-Patch-Name"); newName != "" {
					newChild.Name = newName
				}

				if smode := r.Header.Get("BlobStash-Filetree-Patch-Mode"); smode != "" {
					newMode, err := strconv.ParseInt(smode, 10, 0)
					if err != nil {
						panic(err)
					}
					newChild.Mode = uint32(newMode)
				}

			} else {
				// Decode the raw node from the request body
				newChild = &rnode.RawNode{}
				err = json.NewDecoder(r.Body).Decode(newChild)
			}
			if err != nil {
				panic(err)
			}

			if rename {
				newChild.ChangeTime = mtime
			}
			if !rename && mtime > 0 {
				newChild.ModTime = mtime
				newChild.ChangeTime = 0
			}

			newNode, err := ft.AddChild(ctx, node, newChild, prefixFmt, mtime)
			if err != nil {
				panic(err)
			}

			updateEvent := &FSUpdateEvent{
				Name:      fs.Name,
				Type:      fmt.Sprintf("%s-patched", newChild.Type),
				Ref:       newChild.Hash,
				Path:      filepath.Join(path[1:], newChild.Name),
				Time:      time.Now().UTC().Unix(),
				SessionID: httputil.GetSessionID(r),
			}
			if err := ft.hub.FiletreeFSUpdateEvent(ctx, nil, updateEvent.JSON()); err != nil {
				panic(err)
			}

			httputil.WriteJSON(w, newNode)

		case "DELETE":
			// Delete the node
			node, _, _, err := fs.Path(ctx, path, false, mtime)
			if err != nil {
				if err == blobsfile.ErrBlobNotFound {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				panic(err)
			}

			if hash := r.Header.Get("If-Match"); hash != "" {
				if node.Hash != hash {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}

			// FIXME(tsileo): handle mtime
			if _, err := ft.Delete(ctx, node, prefixFmt, mtime); err != nil {
				panic(err)
			}

			updateEvent := &FSUpdateEvent{
				Name:      fs.Name,
				Type:      fmt.Sprintf("%s-deleted", node.Type),
				Ref:       node.Hash,
				Path:      path[1:],
				Time:      time.Now().UTC().Unix(),
				SessionID: httputil.GetSessionID(r),
			}
			if err := ft.hub.FiletreeFSUpdateEvent(ctx, nil, updateEvent.JSON()); err != nil {
				panic(err)
			}

			w.WriteHeader(http.StatusNoContent)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	}
}

// fileHandler serve the Meta like it's a standard file
func (ft *FileTree) fileHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
		vars := mux.Vars(r)

		hash := vars["ref"]
		ft.serveFile(ctx, w, r, hash)
	}
}

// serveFile serve the node as a file using `net/http` FS util
func (ft *FileTree) serveFile(ctx context.Context, w http.ResponseWriter, r *http.Request, hash string) {
	// FIXME(tsileo): set authorized to true if the API call is authenticated via API key!
	var authorized bool

	if err := bewit.Validate(r, ft.sharingCred); err != nil {
		ft.log.Debug("invalid bewit", "err", err)
	} else {
		ft.log.Debug("valid bewit")
		authorized = true
	}

	blob, err := ft.blobStore.Get(ctx, hash)
	if err != nil {
		if err == clientutil.ErrBlobNotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		panic(err)
	}

	m, err := rnode.NewNodeFromBlob(hash, blob)
	if err != nil {
		panic(err)
	}

	// if !authorized && m.IsPublic() {
	// ft.log.Debug("XAttrs public=1")
	// authorized = true
	// }

	if !authorized {
		// Try if an API key is provided
		ft.log.Info("before authFunc")
		if !ft.authFunc(r) {
			// Rreturns a 404 to prevent leak of hashes
			notFound(w)
			return
		}
	}

	if !m.IsFile() {
		panic(httputil.NewPublicErrorFmt("node is not a file (%s)", m.Type))
	}

	// Initialize a new `File`
	var f io.ReadSeeker
	// FIXME(tsileo): ctx
	f = filereader.NewFile(ctx, ft.blobStore, m, nil)

	// Check if the file is requested for download (?dl=1)
	httputil.SetAttachment(m.Name, r, w)

	// Support for resizing image on the fly
	// var resized bool
	f, _, err = resize.Resize(ft.thumbCache, m.Hash, m.Name, f, r)
	if err != nil {
		panic(err)
	}

	// Serve the file content using the same code as the `http.ServeFile` (it'll handle HEAD request)
	mtime := time.Now() // TODO(ts): use the FS time
	http.ServeContent(w, r, m.Name, mtime, f)
}

// Fetch a Node outside any FS
func (ft *FileTree) nodeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check permissions
		// permissions.CheckPerms(r, PermName)

		// TODO(tsileo): limit the max depth of the tree configurable via query args
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
		vars := mux.Vars(r)

		hash := vars["ref"]
		n, err := ft.nodeByRef(ctx, hash)
		if err != nil {
			if err == clientutil.ErrBlobNotFound {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			panic(err)
		}

		// Output some headers about ACLs
		dlMode := "0"
		if d := r.URL.Query().Get("dl"); d != "" {
			dlMode = d
		}
		u := &url.URL{Path: fmt.Sprintf("/%s/%s", n.Type[0:1], n.Hash)}

		if r.URL.Query().Get("bewit") == "1" {
			if err := bewit.Bewit(ft.sharingCred, u, ft.shareTTL); err != nil {
				panic(err)
			}
			w.Header().Add("BlobStash-FileTree-SemiPrivate-Path", u.String()+"&dl="+dlMode)
			w.Header().Add("BlobStash-FileTree-Bewit", u.Query().Get("bewit"))
			n.URL = u.String() + "?dl=" + dlMode
		}

		if r.Method == "HEAD" {
			return
		}

		if err := ft.fetchDir(ctx, n, 1, 1); err != nil {
			panic(err)
		}

		if r.URL.Query().Get("bewit") == "1" {
			for _, child := range n.Children {
				u := &url.URL{Path: fmt.Sprintf("/%s/%s", child.Type[0:1], child.Hash)}
				if err := bewit.Bewit(ft.sharingCred, u, ft.shareTTL); err != nil {
					panic(err)
				}
				child.URL = u.String() + "&dl=" + dlMode

			}
		}

		httputil.WriteJSON(w, map[string]interface{}{
			"node": n,
		})
	}
}

// nodeByRef fetch the blob containing the `meta.Meta` and convert it to a `Node`
func (ft *FileTree) nodeByRef(ctx context.Context, hash string) (*Node, error) {
	if cached, ok := ft.nodeCache.Get(hash); ok {
		n := cached.(*Node)
		// FIXME(tsileo): investigate a bug in PATCH that cause this
		// if n.Hash != hash {
		// panic("cache messed up")
		// }
		if n.Hash == hash {
			if n.Children != nil && len(n.Children) > 0 {
				n.Children = nil
			}
			return n, nil
		}
	}

	blob, err := ft.blobStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	m, err := rnode.NewNodeFromBlob(hash, blob)
	if err != nil {
		return nil, err
	}

	n, err := MetaToNode(m)
	if err != nil {
		return nil, err
	}

	ft.nodeCache.Add(hash, n)

	return n, nil
}

func (ft *FileTree) Node(ctx context.Context, hash string) (*Node, error) {
	node, err := ft.nodeByRef(ctx, hash)
	if err != nil {
		return nil, err
	}

	f := filereader.NewFile(ctx, ft.blobStore, node.Meta, nil)
	defer f.Close()

	info, err := ft.fetchInfo(f, node.Meta.Name, node.Meta.Hash)
	if err != nil {
		return nil, err
	}
	node.Info = info

	return node, nil
}

// Dummy hanler for 404 responses
func notFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, "<!doctype html><title>BlobStash</title><p>%s</p>\n", http.StatusText(http.StatusNotFound))

}

// dirHandler serve the directory like a standard directory (an HTML page with links to each Node)
func (ft *FileTree) dirHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
		var authorized bool

		if err := bewit.Validate(r, ft.sharingCred); err != nil {
			ft.log.Debug("invalid bewit", "err", err)
		} else {
			ft.log.Debug("valid bewit")
			authorized = true
		}

		vars := mux.Vars(r)

		hash := vars["ref"]
		n, err := ft.nodeByRef(ctx, hash)
		if err != nil {
			if err == clientutil.ErrBlobNotFound {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			panic(err)
		}

		if !authorized {
			// Returns a 404 to prevent leak of hashes
			ft.log.Info("Unauthorized access")
			notFound(w)
			return
		}

		if n.Type != "dir" {
			panic(httputil.NewPublicErrorFmt("node is not a dir (%s)", n.Type))
		}

		if r.Method == "HEAD" {
			return
		}

		if err := ft.fetchDir(ctx, n, 1, 1); err != nil {
			panic(err)
		}

		sort.Sort(byName(n.Children))

		// Check if the dir contains an "index.html")
		for _, cn := range n.Children {
			if cn.Name == indexFile {
				ft.serveFile(ctx, w, r, cn.Hash)
				return
			}
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, "<!doctype html><title>BlobStash - %s</title><pre>\n", n.Name)
		for _, cn := range n.Children {
			u := &url.URL{Path: fmt.Sprintf("/%s/%s", cn.Type[0:1], cn.Hash)}

			// Only compute the Bewit if the node is not public
			if err := bewit.Bewit(ft.sharingCred, u, ft.shareTTL); err != nil {
				panic(err)
			}
			fmt.Fprintf(w, "<a href=\"%s\">%s</a>\n", u.String(), cn.Name)
		}
		fmt.Fprintf(w, "</pre>\n")
	}
}
