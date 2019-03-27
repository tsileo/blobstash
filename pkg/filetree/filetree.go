package filetree // import "a4.io/blobstash/pkg/filetree"

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"container/list"
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
	"a4.io/blobstash/pkg/auth"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/cache"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/ctxutil"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/imginfo"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
	"a4.io/blobstash/pkg/filetree/vidinfo"
	"a4.io/blobstash/pkg/filetree/writer"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/httputil/bewit"
	"a4.io/blobstash/pkg/httputil/resize"
	"a4.io/blobstash/pkg/hub"
	"a4.io/blobstash/pkg/perms"
	"a4.io/blobstash/pkg/queue"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
)

var (
	// FIXME(tsileo): add a way to set a custom fmt key life for Blobs CLI as we don't care about the FS?
	FSKeyFmt = "_filetree:fs:%s"

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
	kvStore   store.KvStore
	blobStore store.BlobStore

	conf *config.Config

	hub *hub.Hub

	authFunc    func(*http.Request) bool
	sharingCred *bewit.Cred
	shareTTL    time.Duration

	thumbCache    *cache.Cache
	metadataCache *cache.Cache
	nodeCache     *lru.Cache
	webmQueue     *queue.Queue

	log log.Logger
}

func (ft *FileTree) SharingCred() *bewit.Cred {
	return ft.sharingCred
}

func (ft *FileTree) ShareTTL() time.Duration {
	return ft.shareTTL
}

// BlobStore is the interface to be compatible with both the server and the BlobStore client
func NewBlobStoreCompat(bs store.BlobStore, ctx context.Context) *BlobStore {
	return &BlobStore{bs, ctx}
}

type BlobStore struct {
	blobStore store.BlobStore
	ctx       context.Context
}

func (bs *BlobStore) Get(hash string) ([]byte, error) {
	return bs.blobStore.Get(bs.ctx, hash)
}

func (bs *BlobStore) Stat(ctx context.Context, hash string) (bool, error) {
	return bs.blobStore.Stat(bs.ctx, hash)
}

func (bs *BlobStore) Put(ctx context.Context, hash string, data []byte) error {
	_, err := bs.blobStore.Put(ctx, &blob.Blob{Hash: hash, Data: data})
	return err
}

// TODO(tsileo): a way to create a snapshot without modifying anything (and forcing the datactx before)
type Snapshot struct {
	Ref       string `msgpack:"-" json:"ref"`
	CreatedAt int64  `msgpack:"-" json:"created_at"`

	Hostname string `msgpack:"h" json:"hostname,omitempty"`
	Message  string `msgpack:"m,omitempty" json:"message,omitempty"`
}

type FS struct {
	Name     string `json:"-"`
	Ref      string `json:"ref"`
	AsOf     int64  `json:"-"`
	Revision int64  `json:"-"`

	ft *FileTree
}

func NewFS(ref string, ft *FileTree) *FS {
	return &FS{Ref: ref, ft: ft}
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

	webmQueue, err := queue.New(filepath.Join(conf.VarDir(), "filetree-webm.queue"))
	if err != nil {
		return nil, err
	}

	ft := &FileTree{
		conf:      conf,
		kvStore:   kvStore,
		blobStore: blobStore,
		sharingCred: &bewit.Cred{
			Key: []byte(conf.SharingKey),
			ID:  "filetree",
		},
		webmQueue:     webmQueue,
		thumbCache:    thumbscache,
		metadataCache: metacache,
		nodeCache:     nodeCache,
		authFunc:      authFunc,
		shareTTL:      1 * time.Hour,
		hub:           chub,
		log:           logger,
	}

	chub.Subscribe(hub.NewFiletreeNode, "webm", ft.webmHubCallback)
	go ft.webmWorker()

	return ft, nil
}

// Close closes all the open DB files.
func (ft *FileTree) Close() error {
	ft.thumbCache.Close()
	ft.metadataCache.Close()
	return nil
}

func (ft *FileTree) webmWorker() {
	log := ft.log.New("worker", "webm_worker")
	log.Debug("starting worker")
	n := &rnode.RawNode{}
L:
	for {
		select {
		//case <-ft.stop:
		//	log.Debug("worker stopped")
		//	break L
		default:
			ok, deqFunc, err := ft.webmQueue.Dequeue(n)
			if err != nil {
				panic(err)
			}
			if ok {
				if err := func(n *rnode.RawNode) error {
					if n.Size == 0 {
						deqFunc(true)
						log.Error(fmt.Sprintf("dropping webm task %+v", n), "ref", n.Hash)
						return nil
					}
					log.Info(fmt.Sprintf("starting %+v", n), "ref", n.Hash)
					if !vidinfo.IsVideo(n.Name) {
						log.Error("not a vid")
						deqFunc(true)
						return nil
					}

					t := time.Now()
					//b.wg.Add(1)
					//defer b.wg.Done()

					oPath := filepath.Join(os.TempDir(), n.ContentHash)
					if err := filereader.GetFile(context.Background(), ft.blobStore, n.Hash, oPath); err != nil {
						return err
					}
					// defer os.Remove(oPath)
					info, err := vidinfo.Parse(oPath)
					if err != nil {
						return err
					}
					log.Info(fmt.Sprintf("got info=%+v", info), "ref", n.Hash)
					js, err := json.Marshal(info)
					if err != nil {
						return err
					}
					if err := ioutil.WriteFile(vidinfo.InfoPath(ft.conf, n.ContentHash), js, 0666); err != nil {
						return err
					}
					log.Info("Caching", "ref", n.Hash)
					if err := vidinfo.Cache(ft.conf, oPath, n.ContentHash, info.Duration); err != nil {
						ft.log.Error("failed to cache", "err", err.Error())
						return err
					}

					deqFunc(true)
					log.Info("video processed", "ref", n.Hash, "duration", time.Since(t))
					return nil
				}(n); err != nil {
					log.Error("failed to process video", "ref", n.Hash, "err", err)
					time.Sleep(1 * time.Second)
				}
				continue L
			}
			time.Sleep(1 * time.Second)
			continue L
		}
	}
}

func (ft *FileTree) webmHubCallback(ctx context.Context, _ *blob.Blob, data interface{}) error {
	n := data.(*rnode.RawNode)
	fmt.Printf("NODE=%+v\n", data)
	if vidinfo.IsVideo(n.Name) {
		if _, err := os.Stat(vidinfo.WebmPath(ft.conf, n.ContentHash)); os.IsNotExist(err) {
			ft.log.Info("Webm callback", "ref", n.Hash)
			if _, err := ft.webmQueue.Enqueue(n); err != nil {
				ft.log.Error("failed to enqueue", "err", err.Error())
				return err
			}
			ft.log.Info("enqueued for webm processing", "ref", n.Hash)
		}
	}
	return nil
}

// RegisterRoute registers all the HTTP handlers for the extension
func (ft *FileTree) Register(r *mux.Router, root *mux.Router, basicAuth func(http.Handler) http.Handler) {
	// Raw node endpoint
	r.Handle("/node/{ref}", basicAuth(http.HandlerFunc(ft.nodeHandler())))
	r.Handle("/node/{ref}/_snapshot", basicAuth(http.HandlerFunc(ft.nodeSnapshotHandler())))
	r.Handle("/node/{ref}/_search", basicAuth(http.HandlerFunc(ft.nodeSearchHandler())))

	// TODO(ts): deprecate this endpoint and use commit /_snapshot?
	r.Handle("/commit/{type}/{name}", basicAuth(http.HandlerFunc(ft.commitHandler())))

	r.Handle("/versions/{type}/{name}", basicAuth(http.HandlerFunc(ft.versionsHandler())))

	r.Handle("/fs", basicAuth(http.HandlerFunc(ft.fsRootHandler())))
	r.Handle("/fs/{type}/{name}/_tree_blobs", basicAuth(http.HandlerFunc(ft.treeBlobsHandler())))
	r.Handle("/fs/{type}/{name}/_tgz", basicAuth(http.HandlerFunc(ft.tgzHandler())))
	r.Handle("/fs/{type}/{name}/", basicAuth(http.HandlerFunc(ft.fsHandler())))
	r.Handle("/fs/{type}/{name}/{path:.+}", basicAuth(http.HandlerFunc(ft.fsHandler())))
	// r.Handle("/fs", http.HandlerFunc(ft.fsHandler()))
	// r.Handle("/fs/{name}", http.HandlerFunc(ft.fsByNameHandler()))

	root.Handle("/public/{type}/{name}/", http.HandlerFunc(ft.publicHandler()))
	root.Handle("/public/{type}/{name}/{path:.+}", http.HandlerFunc(ft.publicHandler()))

	r.Handle("/upload", basicAuth(http.HandlerFunc(ft.uploadHandler())))

	// Public/semi-private handler
	fileHandler := http.HandlerFunc(ft.fileHandler())
	// Hook the standard endpint
	r.Handle("/file/{ref}", fileHandler)
	// Enable shortcut path from the root
	root.Handle("/f/{ref}", fileHandler)
	root.Handle("/w/{ref}.{ext}", http.HandlerFunc(ft.webmHandler()))
}

// Node holds the data about the file node (either file/dir), analog to a Meta
type Node struct {
	Name          string  `json:"name" msgpack:"n"`
	Type          string  `json:"type" msgpack:"t"`
	Size          int     `json:"size,omitempty" msgpack:"s,omitempty"`
	Mode          int     `json:"mode,omitempty" msgpack:"mo,omitempty"`
	ModTime       string  `json:"mtime" msgpack:"mt"`
	ChangeTime    string  `json:"ctime" msgpack:"ct"`
	ContentHash   string  `json:"content_hash,omitempty" msgpack:"ch,omitempty"`
	Hash          string  `json:"ref" msgpack:"r"`
	Children      []*Node `json:"children,omitempty" msgpack:"c,omitempty"`
	ChildrenCount int     `json:"children_count,omitempty" msgpack:"cc,omitempty"`

	// FIXME(ts): rename to Metadata
	Data map[string]interface{} `json:"metadata,omitempty" msgpack:"md,omitempty"`
	Info *Info                  `json:"info,omitempty" msgpack:"i,omitempty"`

	Meta   *rnode.RawNode `json:"-" msgpack:"-"`
	parent *Node
	fs     *FS

	URL  string            `json:"url,omitempty" msgpack:"u,omitempty"`
	URLs map[string]string `json:"urls,omitempty" msgpack:"us,omitempty"`
}

// Update the given node with the given meta, the updated/new node is assumed to be already saved
func (ft *FileTree) Update(ctx context.Context, n *Node, m *rnode.RawNode, prefixFmt string, first bool) (*Node, int64, error) {
	newNode, err := MetaToNode(m)
	if err != nil {
		return nil, 0, err
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
			return nil, 0, err
		}
		newRev, err := ft.kvStore.Put(ctx, fmt.Sprintf(prefixFmt, n.fs.Name), newNode.Hash, snapEncoded, -1)
		if err != nil {
			return nil, 0, err
		}
		return newNode, newRev.Version, nil
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
	if _, err := ft.blobStore.Put(ctx, &blob.Blob{Hash: newRef, Data: data}); err != nil {
		return nil, 0, err
	}
	// n.parent.Hash = newRef
	// parentMeta.Hash = newRef
	// Propagate the change to the parents
	_, kvVersion, err := ft.Update(ctx, newNode.parent, newNode.parent.Meta, prefixFmt, false)
	if err != nil {
		return nil, 0, err
	}
	return newNode, kvVersion, nil
}

// Update the given node with the given meta, the updated/new node is assumed to be already saved
func (ft *FileTree) AddChild(ctx context.Context, n *Node, newChild *rnode.RawNode, prefixFmt string, mtime int64) (*Node, int64, error) {
	// Save the new child meta
	//newChild.ModTime = time.Now().UTC().Unix()
	newChildRef, data := newChild.Encode()
	newChild.Hash = newChildRef
	if _, err := ft.blobStore.Put(ctx, &blob.Blob{Hash: newChildRef, Data: data}); err != nil {
		return nil, 0, err
	}
	newChildNode, err := MetaToNode(newChild)
	if err != nil {
		return nil, 0, err
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
	if _, err := ft.blobStore.Put(ctx, &blob.Blob{Hash: newRef, Data: data}); err != nil {
		return nil, 0, err
	}

	// Proagate the change up to the ~moon~ root
	return ft.Update(ctx, n, n.Meta, prefixFmt, true)
}

// Delete removes the given node from its parent children
func (ft *FileTree) Delete(ctx context.Context, n *Node, prefixFmt string, mtime int64) (*Node, int64, error) {
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
	parent.ChildrenCount = len(newChildren)
	newRef, data := parent.Meta.Encode()
	parent.Hash = newRef
	parent.Meta.Hash = newRef
	if _, err := ft.blobStore.Put(ctx, &blob.Blob{Hash: newRef, Data: data}); err != nil {
		return nil, 0, err
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
	if m.Version != rnode.V1 {
		return nil, fmt.Errorf("bad node version \"%s\" for node %+v", m.Version, m)
	}
	//if m.Name == "_root" {
	// TODO(tsileo): the FS.name
	//}
	n := &Node{
		Name:        m.Name,
		Type:        m.Type,
		Size:        m.Size,
		Data:        m.Metadata,
		Hash:        m.Hash,
		ContentHash: m.ContentHash,
		Mode:        int(m.Mode),
		Meta:        m,
	}
	if n.Type == rnode.Dir {
		n.ChildrenCount = len(m.Refs)
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

// IterFS iterates the whole FS tree and executes `fn` on each node
func (ft *FileTree) IterTree(ctx context.Context, root *Node, fn func(*Node, string) error) error {
	return ft.iterTree(ctx, ft.DFS, root, fn)
}

// GraphSearchFunc is the interface type for the different graph transversal algorithms (BFS,DFS)
type GraphSearchFunc = func(context.Context, *Node, func(*Node) (bool, error)) (*Node, error)

// iterFS iterates the whole tree using the given graph search algo and executes `fn` on each node
func (ft *FileTree) iterTree(ctx context.Context, sfunc GraphSearchFunc, root *Node, fn func(*Node, string) error) error {
	if _, err := sfunc(ctx, root, func(n *Node) (bool, error) {
		p := nodePath(n, root)
		if err := fn(n, p); err != nil {
			// Stop the search
			return true, err
		}
		return false, nil
	}); err != nil {
		return err
	}
	return nil
}

// DFS performs a (recursive) Depth-first search
func (ft *FileTree) DFS(ctx context.Context, root *Node, fn func(*Node) (bool, error)) (*Node, error) {
	// Check if the target is the root
	found, err := fn(root)
	if err != nil {
		return nil, err
	}
	if found {
		return root, nil
	}

	if root.Type == rnode.File {
		return nil, nil
	}

	// Check each children as we discover it, and expand a directory as soon as it's discovered
	for _, ref := range root.Meta.Refs {
		cn, err := ft.nodeByRef(ctx, ref.(string))
		if err != nil {
			return nil, err
		}
		cn.parent = root
		nfound, err := ft.DFS(ctx, cn, fn)
		if err != nil {
			return nil, err
		}
		if nfound != nil {
			return nfound, nil
		}
	}

	return nil, nil
}

// BFS performs a Breadth-first search
func (ft *FileTree) BFS(ctx context.Context, root *Node, fn func(*Node) (bool, error)) (*Node, error) {
	// create a FIFO queue for the graph traversal
	// (list + PushBash + Font)
	q := list.New()
	q.PushBack(root)

	for q.Len() > 0 {
		// Dequeue
		e := q.Front()
		n := e.Value.(*Node)
		q.Remove(e)

		// Check if the target is the root
		found, err := fn(n)
		if err != nil {
			return nil, err
		}
		if found {
			return n, nil
		}

		// Enqueue the children to be expanded
		if n.Type == rnode.Dir {
			for _, ref := range n.Meta.Refs {
				cn, err := ft.nodeByRef(ctx, ref.(string))
				if err != nil {
					return nil, err
				}
				cn.parent = n
				q.PushBack(cn)
			}
		}
	}

	return nil, nil
}

// fetchDir recursively fetch dir children
func (ft *FileTree) fetchDir(ctx context.Context, n *Node, depth, maxDepth int) error {
	if depth > maxDepth {
		return nil
	}
	if n.Type == rnode.Dir {
		n.Children = []*Node{}
		for _, ref := range n.Meta.Refs {
			cn, err := ft.nodeByRef(ctx, ref.(string))
			if err != nil {
				return err
			}
			if cn.Type == "file" {
				// FIXME(tsileo): init the new file in fetchInfo and only if needed
				f := filereader.NewFile(ctx, ft.blobStore, cn.Meta, nil)
				defer f.Close()

				info, err := ft.fetchInfo(f, cn.Meta.Name, cn.Meta.Hash, cn.Meta.ContentHash)
				if err != nil {
					panic(err)
				}
				cn.Info = info
			}

			n.Children = append(n.Children, cn)
			if err := ft.fetchDir(ctx, cn, depth+1, maxDepth); err != nil {
				return err
			}
		}
	}

	sort.Slice(n.Children, func(i, j int) bool {
		return n.Children[i].Meta.Name < n.Children[j].Meta.Name
	})

	return nil
}

// Commit duplicate the last snapshot and add a commit message
func (fs *FS) commit(ctx context.Context, prefixFmt, message string) (int64, error) {
	kv, err := fs.ft.kvStore.Get(ctx, fmt.Sprintf(prefixFmt, fs.Name), -1)
	if err != nil && err != vkv.ErrNotFound {
		return 0, err
	}
	snap := &Snapshot{}
	if err := msgpack.Unmarshal(kv.Data, snap); err != nil {
		return 0, err
	}
	snap.Message = message

	snapEncoded, err := msgpack.Marshal(snap)
	if err != nil {
		return 0, err
	}
	newRev, err := fs.ft.kvStore.Put(ctx, fmt.Sprintf(prefixFmt, fs.Name), kv.HexHash(), snapEncoded, -1)
	if err != nil {
		return 0, err
	}

	return newRev.Version, nil

}

// FS fetch the FileSystem by name, returns an empty one if not found
func (ft *FileTree) FS(ctx context.Context, name, prefixFmt string, newState bool, asOf int64) (*FS, error) {
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
				fs.Revision = kv.Version
			case vkv.ErrNotFound:
				// XXX(tsileo): should the `ErrNotFound` be returned here?
			default:
				return nil, err
			}
		} else {
			// Set the existing ref
			kvv, _, err := ft.kvStore.Versions(ctx, fmt.Sprintf(prefixFmt, name), strconv.FormatInt(asOf, 10), 1)
			switch err {
			case nil:
				if len(kvv.Versions) > 0 {
					fs.Ref = kvv.Versions[0].HexHash()
					fs.Revision = kvv.Versions[0].Version
				}

			case vkv.ErrNotFound:
				// XXX(tsileo): should the `ErrNotFound` be returned here?
			default:
				return nil, err
			}
		}
	}
	fs.Name = name
	fs.AsOf = asOf
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
			Type:    rnode.Dir,
			Version: rnode.V1,
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
func (fs *FS) Path(ctx context.Context, path string, depth int, create bool, mtime int64) (*Node, *rnode.RawNode, bool, error) {
	var found bool
	node, err := fs.Root(ctx, create, mtime)
	if err != nil {
		return nil, nil, found, err
	}
	var prev *Node
	var cmeta *rnode.RawNode
	node.fs = fs
	node.parent = nil
	if path == "/" {
		if err := fs.ft.fetchDir(ctx, node, 1, depth); err != nil {
			return nil, nil, found, err
		}

		fs.ft.log.Info("returning root")
		return node, cmeta, found, err
	} else {
		if err := fs.ft.fetchDir(ctx, node, 1, 1); err != nil {
			return nil, nil, found, err
		}

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
				//if err := fs.ft.fetchDir(ctx, node, 1, 1); err != nil {
				//	return nil, nil, found, err
				//}
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
				Type:    rnode.Dir,
				Version: rnode.V1,
				Name:    p,
				ModTime: mtime,
			}
			if i == pathCount-1 {
				cmeta.Type = rnode.File
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
	if err := fs.ft.fetchDir(ctx, node, 1, depth); err != nil {
		return nil, nil, found, err
	}

	return node, cmeta, !found, nil
}

// Handle multipart form upload to create a new Node (outside of any FS)
func (ft *FileTree) uploadHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
		ctx = ctxutil.WithNamespace(ctx, r.Header.Get(ctxutil.NamespaceHeader))
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
		info, err := ft.fetchInfo(reader, handler.Filename, meta.Hash, meta.ContentHash)
		if err != nil {
			panic(err)
		}
		node, err := MetaToNode(meta)
		if err != nil {
			panic(err)
		}
		node.Info = info
		httputil.MarshalAndWrite(r, w, node)
	}
}

type Info struct {
	Image *imginfo.Image `json:"image,omitempty" msgpack:"image,omitempty"`
	Video *vidinfo.Video `json:"video,omitempty" msgpack:"video,omitempty"`
}

func (ft *FileTree) fetchInfo(reader io.ReadSeeker, filename, hash, contentHash string) (*Info, error) {
	fmt.Printf("FETCHINFO %s\n%s\n\n\n", filename, hash)
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
	fmt.Printf("lname=%v\n", lname)
	if vidinfo.IsVideo(filename) {
		infoPath := vidinfo.InfoPath(ft.conf, contentHash)
		if _, err := os.Stat(infoPath); err == nil {
			js, err := ioutil.ReadFile(infoPath)
			if err != nil {
				return nil, err
			}
			videoInfo := &vidinfo.Video{}
			if err := json.Unmarshal(js, videoInfo); err != nil {
				return nil, err
			}
			info.Video = videoInfo
		} else {
			// ffmpeg may still be running, don't cache the "no result"
			return info, nil
		}
	}
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
		it, err := ft.IterFS(ctx, prefix)
		if err != nil {
			panic(err)
		}
		for _, fsInfo := range it {
			fmt.Printf("fsInfo=%+v\n", fsInfo)
			fs := &FS{Name: fsInfo.Name, Ref: fsInfo.Ref, ft: ft}
			node, _, _, err := fs.Path(ctx, "/", 1, false, 0)
			if err != nil {
				panic(err)
			}
			nodes = append(nodes, node)
		}
		httputil.MarshalAndWrite(r, w, nodes)
	}
}

type FSInfo struct {
	Name string
	Ref  string
}

func (ft *FileTree) IterFS(ctx context.Context, start string) ([]*FSInfo, error) {
	out := []*FSInfo{}

	prefix := fmt.Sprintf("_filetree:fs:%s", start)
	keys, _, err := ft.kvStore.Keys(ctx, prefix, prefix+"\xff", 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range keys {
		data := strings.Split(kv.Key, ":")
		fsInfo := &FSInfo{Name: data[len(data)-1], Ref: kv.HexHash()}
		out = append(out, fsInfo)
	}
	return out, nil
}

func fixPath(p string) string {
	if p == "." {
		return ""
	}
	return p
}

func (ft *FileTree) versionsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)

		}
		ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))

		vars := mux.Vars(r)
		fsName := vars["name"]
		refType := vars["type"]
		prefixFmt := FSKeyFmt
		if p := r.URL.Query().Get("prefix"); p != "" {
			prefixFmt = p + ":%s"
		}

		var err error
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

		q := httputil.NewQuery(r.URL.Query())

		limit, err := q.GetInt("limit", 50, 1000)
		if err != nil {
			panic(err)
		}

		kvv, _, err := ft.kvStore.Versions(ctx, fmt.Sprintf(prefixFmt, fs.Name), "0", -1)
		switch err {
		case nil:
		case vkv.ErrNotFound:
		default:
			panic(err)
		}
		versions := []*Snapshot{}

		// the key may not exists
		if kvv != nil {
			for _, kv := range kvv.Versions {
				snap := &Snapshot{
					CreatedAt: kv.Version,
					Ref:       kv.HexHash(),
				}
				if err := msgpack.Unmarshal(kv.Data, snap); err != nil {
					panic(err)
				}
				versions = append(versions, snap)
				if len(versions) == limit {
					break
				}
			}
		}

		httputil.MarshalAndWrite(r, w, map[string]interface{}{
			"versions": versions,
		})
		return
	}
}

func (ft *FileTree) commitHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		ctx := r.Context()

		vars := mux.Vars(r)
		fsName := vars["name"]
		refType := vars["type"]
		prefixFmt := FSKeyFmt
		if p := r.URL.Query().Get("prefix"); p != "" {
			prefixFmt = p + ":%s"
		}

		var err error
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

		message, err := httputil.Read(r)
		if err != nil {
			panic(err)
		}

		revision, err := fs.commit(ctx, prefixFmt, string(message))
		if err != nil {
			panic(err)
		}

		w.Header().Add("BlobStash-Filetree-FS-Revision", strconv.FormatInt(revision, 10))
		w.WriteHeader(http.StatusNoContent)

	}
}

func (ft *FileTree) fsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := ctxutil.WithFileTreeHostname(r.Context(), r.Header.Get(ctxutil.FileTreeHostnameHeader))
		ctx = ctxutil.WithNamespace(ctx, r.Header.Get(ctxutil.NamespaceHeader))

		// FIXME(tsileo): handle mtime in the context too, and make it optional

		vars := mux.Vars(r)
		fsName := vars["name"]
		path := "/" + vars["path"]
		refType := vars["type"]
		prefixFmt := FSKeyFmt
		if p := r.URL.Query().Get("prefix"); p != "" {
			prefixFmt = p + ":%s"
		}
		var mtime, asOf int64
		var err error
		q := httputil.NewQuery(r.URL.Query())

		mtime, err = q.GetInt64Default("mtime", 0)
		if err != nil {
			panic(err)
		}
		asOf, err = q.GetInt64Default("as_of", 0)
		if err != nil {
			panic(err)
		}
		depth, err := q.GetInt("depth", 1, 5)
		if err != nil {
			panic(err)
		}

		var fs *FS
		switch refType {
		case "ref":
			fs = &FS{
				Ref: fsName,
				ft:  ft,
			}
		case "fs":
			fs, err = ft.FS(ctx, fsName, prefixFmt, false, asOf)
			if err != nil {
				panic(err)
			}
		default:
			panic(fmt.Errorf("Unknown type \"%s\"", refType))
		}
		switch r.Method {
		case "GET", "HEAD":
			node, _, _, err := fs.Path(ctx, path, depth, false, mtime)
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
				panic(fmt.Errorf("failed to get path: %v", err))
			}

			w.Header().Set("ETag", node.Hash)
			w.Header().Set("BlobStash-FileTree-Revision", strconv.FormatInt(fs.Revision, 10))

			// Handle HEAD request
			if r.Method == "HEAD" {
				return
			}

			if node.Type == "file" {
				// FIXME(tsileo): init the new file in fetchInfo and only if needed
				f := filereader.NewFile(ctx, ft.blobStore, node.Meta, nil)
				defer f.Close()

				fmt.Printf("METAMAETA=%+v\n", node.Meta)
				info, err := ft.fetchInfo(f, node.Meta.Name, node.Meta.Hash, node.Meta.ContentHash)
				if err != nil {
					panic(err)
				}
				node.Info = info
			}
			// Returns the Node as JSON
			httputil.MarshalAndWrite(r, w, node)
			return

		case "POST":
			// FIXME(tsileo): add a way to upload a file as public ? like AWS S3 public-read canned ACL
			// Add a new node in the FS at the given path
			node, _, created, err := fs.Path(ctx, path, 1, true, mtime)
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
			newNode, revision, err := ft.Update(ctx, node, meta, prefixFmt, true)
			if err != nil {
				panic(err)
			}

			w.Header().Add("BlobStash-Filetree-FS-Revision", strconv.FormatInt(revision, 10))

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

			httputil.MarshalAndWrite(r, w, newNode)
			return

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
			node, _, _, err := fs.Path(ctx, path, 1, true, mtime)
			if err != nil {
				if err == blobsfile.ErrBlobNotFound {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				panic(err)
			}
			if node.Type != rnode.Dir {
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
				if smodtime := r.Header.Get("BlobStash-Filetree-Patch-ModTime"); smodtime != "" {
					newModTime, err := strconv.ParseInt(smodtime, 10, 0)
					if err != nil {
						panic(err)
					}
					newChild.ModTime = int64(newModTime)
				}

			} else {
				// Decode the raw node from the request body
				newChild = &rnode.RawNode{}
				err = httputil.Unmarshal(r, newChild)
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

			newNode, revision, err := ft.AddChild(ctx, node, newChild, prefixFmt, mtime)
			if err != nil {
				panic(err)
			}

			w.Header().Add("BlobStash-Filetree-FS-Revision", strconv.FormatInt(revision, 10))

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

			httputil.MarshalAndWrite(r, w, newNode)
			return

		case "DELETE":
			// Delete the node
			node, _, _, err := fs.Path(ctx, path, 1, false, mtime)
			if err != nil {
				if err == blobsfile.ErrBlobNotFound {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				// FIXME FIXME FIXME
				w.WriteHeader(http.StatusNotFound)
				return
				//panic(err)
			}

			if hash := r.Header.Get("If-Match"); hash != "" {
				if node.Hash != hash {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}

			_, revision, err := ft.Delete(ctx, node, prefixFmt, mtime)
			if err != nil {
				panic(err)
			}

			w.Header().Add("BlobStash-Filetree-FS-Revision", strconv.FormatInt(revision, 10))

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
			return

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	}
}

func (ft *FileTree) tgzHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := ctxutil.WithFileTreeHostname(r.Context(), r.Header.Get(ctxutil.FileTreeHostnameHeader))
		ctx = ctxutil.WithNamespace(ctx, r.Header.Get(ctxutil.NamespaceHeader))

		// FIXME(tsileo): handle mtime in the context too, and make it optional

		vars := mux.Vars(r)
		fsName := vars["name"]
		path := "/" + vars["path"]
		refType := vars["type"]
		prefixFmt := FSKeyFmt
		if p := r.URL.Query().Get("prefix"); p != "" {
			prefixFmt = p + ":%s"
		}
		var mtime, asOf int64
		var err error
		q := httputil.NewQuery(r.URL.Query())

		asOf, err = q.GetInt64Default("as_of", 0)
		if err != nil {
			panic(err)
		}

		var fs *FS
		switch refType {
		case "ref":
			fs = &FS{
				Ref: fsName,
				ft:  ft,
			}
		case "fs":
			fs, err = ft.FS(ctx, fsName, prefixFmt, false, asOf)
			if err != nil {
				panic(err)
			}
		default:
			panic(fmt.Errorf("Unknown type \"%s\"", refType))
		}
		switch r.Method {
		case "GET", "HEAD":
			node, _, _, err := fs.Path(ctx, path, 1, false, mtime)
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
				panic(fmt.Errorf("failed to get path: %v", err))
			}

			w.Header().Set("ETag", node.Hash)
			w.Header().Set("Content-Type", "application/gzip")

			// Handle HEAD request
			if r.Method == "HEAD" {
				return
			}

			gzipWriter := gzip.NewWriter(w)
			tarWriter := tar.NewWriter(gzipWriter)

			// Iter the whole tree
			ctx := context.TODO()
			if err := ft.IterTree(ctx, node, func(n *Node, p string) error {
				// Skip directories (We only want files to be added)
				if !n.Meta.IsFile() {
					return nil
				}

				// Write the tar header
				hdr := &tar.Header{
					Name: p[1:],
					Mode: int64(os.FileMode(n.Mode) | 0600),
					Size: int64(n.Size),
				}
				if err := tarWriter.WriteHeader(hdr); err != nil {
					panic(err)
				}

				// write the file content (iter over all the blobs)
				for _, iv := range n.Meta.FileRefs() {
					blob, err := ft.blobStore.Get(ctx, iv.Value)
					if err != nil {
						panic(err)
					}
					if _, err := tarWriter.Write(blob); err != nil {
						panic(err)
					}
				}
				return nil
			}); err != nil {
				panic(err)
			}

			// "seal" the tarfile
			tarWriter.Close()
			gzipWriter.Close()

			// TODO(tsileo): set attachment headder
		}
	}
}

func (ft *FileTree) treeBlobsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()
		vars := mux.Vars(r)
		fsName := vars["name"]
		path := "/" + vars["path"]
		refType := vars["type"]
		prefixFmt := FSKeyFmt
		if p := r.URL.Query().Get("prefix"); p != "" {
			prefixFmt = p + ":%s"
		}

		if path != "/" {
			panic("can only tree blobs the root path")
		}

		var asOf int64
		var err error
		q := httputil.NewQuery(r.URL.Query())

		asOf, err = q.GetInt64Default("as_of", 0)
		if err != nil {
			panic(err)
		}

		var fs *FS
		switch refType {
		case "ref":
			fs = &FS{
				Ref: fsName,
				ft:  ft,
			}
		case "fs":
			fs, err = ft.FS(ctx, fsName, prefixFmt, false, asOf)
			if err != nil {
				panic(err)
			}
		default:
			panic(fmt.Errorf("Unknown type \"%s\"", refType))
		}

		node, _, _, err := fs.Path(ctx, "/", 1, false, 0)
		if err != nil {
			panic(fmt.Errorf("failed to get path: %v", err))
		}

		tree, err := ft.TreeBlobs(ctx, node)
		if err != nil {
			panic(err)
		}
		fmt.Printf("tree_len=%d\n", len(tree))
		httputil.MarshalAndWrite(r, w, map[string]interface{}{
			"data": tree,
		})
	}
}

func (ft *FileTree) TreeBlobs(ctx context.Context, node *Node) ([]string, error) {
	// FIXME(tsileo): take a FS, a fix the path arg
	out := []string{}
	if err := ft.IterTree(ctx, node, func(n *Node, p string) error {
		out = append(out, n.Meta.Hash)

		// Skip directories (the children will be iterated as part of iter tree)
		if !n.Meta.IsFile() {
			return nil
		}

		// write the file content (iter over all the blobs)
		for _, iv := range n.Meta.FileRefs() {
			out = append(out, iv.Value)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return out, nil
}

func (ft *FileTree) webmHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		vars := mux.Vars(r)

		hash := vars["ref"]
		ext := vars["ext"]

		var authorized bool
		if err := bewit.Validate(r, ft.sharingCred); err != nil {
			ft.log.Debug("invalid bewit", "err", err)
		} else {
			ft.log.Debug("valid bewit")
			authorized = true
		}

		if !authorized {
			// Try if an API key is provided
			ft.log.Info("before authFunc")
			if !ft.authFunc(r) {
				// Rreturns a 404 to prevent leak of hashes
				notFound(w)
				return
			}
		}
		webmPath := filepath.Join(ft.conf.VidDir(), fmt.Sprintf("%s.%s", hash, ext))
		fmt.Printf("webmPath=%s\n", webmPath)
		if _, err := os.Stat(webmPath); err != nil {
			w.WriteHeader(404)
			return
		}
		http.ServeFile(w, r, webmPath)
		return
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
		ft.serveFile(ctx, w, r, hash, false)
	}
}

// serveFile serve the node as a file using `net/http` FS util
func (ft *FileTree) serveFile(ctx context.Context, w http.ResponseWriter, r *http.Request, hash string, authorized bool) {
	// FIXME(tsileo): set authorized to true if the API call is authenticated via API key!

	if err := bewit.Validate(r, ft.sharingCred); err != nil {
		ft.log.Debug("invalid bewit", "err", err)
	} else {
		ft.log.Debug("valid bewit")
		authorized = true
	}

	if !authorized {
		// Try if an API key is provided
		ft.log.Info("before authFunc")
		if !ft.authFunc(r) {
			// Rreturns a 404 to prevent leak of hashes
			notFound(w)
			return
		}
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

	var mtime time.Time
	if m.ModTime > 0 {
		mtime = time.Unix(m.ModTime, 0)
	} else {
		mtime = time.Now()
	}

	// Serve the file content using the same code as the `http.ServeFile` (it'll handle HEAD request)
	http.ServeContent(w, r, m.Name, mtime, f)
}

func (ft *FileTree) publicHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		ctx := ctxutil.WithFileTreeHostname(r.Context(), r.Header.Get(ctxutil.FileTreeHostnameHeader))
		ctx = ctxutil.WithNamespace(ctx, r.Header.Get(ctxutil.NamespaceHeader))

		vars := mux.Vars(r)
		fsName := vars["name"]
		path := "/public/" + vars["path"]
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
		node, _, _, err := fs.Path(ctx, path, 1, false, mtime)
		switch err {
		case nil:
		case clientutil.ErrBlobNotFound:
			// Returns a 404 if the blob/children is not found
			notFound(w)
			return
		case blobsfile.ErrBlobNotFound:
			// Returns a 404 if the blob/children is not found
			notFound(w)
			return
		default:
			panic(err)
		}

		w.Header().Set("ETag", node.Hash)

		// Handle HEAD request
		if r.Method == "HEAD" {
			return
		}

		ft.serveFile(ctx, w, r, node.Hash, true)
		return
	}
}

func (ft *FileTree) GetSemiPrivateLink(n *Node) (string, string, error) {
	u := &url.URL{Path: fmt.Sprintf("/%s/%s", n.Type[0:1], n.Hash)}
	if err := bewit.Bewit(ft.sharingCred, u, ft.shareTTL); err != nil {
		panic(err)
	}
	return u.String() + "&dl=1", u.String() + "&dl=0", nil
}

func (ft *FileTree) GetWebmLink(n *Node) (string, string, error) {
	u := &url.URL{Path: fmt.Sprintf("/w/%s.webm", n.ContentHash)}
	if err := bewit.Bewit(ft.sharingCred, u, ft.shareTTL); err != nil {
		panic(err)
	}
	u1 := &url.URL{Path: fmt.Sprintf("/w/%s.jpg", n.ContentHash)}
	if err := bewit.Bewit(ft.sharingCred, u1, ft.shareTTL); err != nil {
		panic(err)
	}
	return u.String(), u1.String(), nil
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

		// FIXME(tsileo): init the new file in fetchInfo and only if needed
		f := filereader.NewFile(ctx, ft.blobStore, n.Meta, nil)
		defer f.Close()

		info, err := ft.fetchInfo(f, n.Meta.Name, n.Meta.Hash, n.Meta.ContentHash)
		if err != nil {
			panic(err)
		}
		n.Info = info

		u1 := &url.URL{Path: fmt.Sprintf("/w/%s.webm", n.ContentHash)}

		if err := bewit.Bewit(ft.sharingCred, u1, ft.shareTTL); err != nil {
			panic(err)
		}
		n.URLs = map[string]string{"webm": u1.String()}

		fmt.Printf("INFO FETCHED")
		httputil.MarshalAndWrite(r, w, map[string]interface{}{
			"node": n,
		})
	}
}

type snapReq struct {
	FS       string `json:"fs"`
	Message  string `json:"message"`
	Hostname string `json:"hostname"`
}

// Fetch a Node outside any FS
func (ft *FileTree) nodeSnapshotHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		sreq := &snapReq{}
		if err := httputil.Unmarshal(r, sreq); err != nil {
			panic(err)
		}

		if !auth.Can(
			w,
			r,
			perms.Action(perms.Snapshot, perms.FS),
			perms.ResourceWithID(perms.Filetree, perms.FS, sreq.FS),
		) {
			auth.Forbidden(w)
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
		if n.Type == "file" {
			panic("cannot snapshot a file")
		}

		snap := &Snapshot{
			Message:  sreq.Message,
			Hostname: sreq.Hostname,
		}

		snapEncoded, err := msgpack.Marshal(snap)
		if err != nil {
			panic(err)
		}
		newRev, err := ft.kvStore.Put(ctx, fmt.Sprintf(FSKeyFmt, sreq.FS), hash, snapEncoded, -1)
		if err != nil {
			panic(err)
		}

		// return newRev.Version, nil
		httputil.MarshalAndWrite(r, w, map[string]interface{}{
			"version": newRev.Version,
			"ref":     hash,
		})
	}
}

// nodeByRef fetch the blob containing the `meta.Meta` and convert it to a `Node`
func (ft *FileTree) nodeByRef(ctx context.Context, hash string) (*Node, error) {
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

	return n, nil
}

func (ft *FileTree) Node(ctx context.Context, hash string) (*Node, error) {
	node, err := ft.nodeByRef(ctx, hash)
	if err != nil {
		return nil, err
	}

	f := filereader.NewFile(ctx, ft.blobStore, node.Meta, nil)
	defer f.Close()

	info, err := ft.fetchInfo(f, node.Meta.Name, node.Meta.Hash, node.Meta.ContentHash)
	if err != nil {
		return nil, err
	}
	node.Info = info

	return node, nil
}

// NodeInfo represents a node for the path/breadcrumbs
type NodeInfo struct {
	Name, Ref string
}

// Takes a child node and the root, return the list of `NodeInfo` from the root until the children
func pathFromNode(ntarget, rootNode *Node) []*NodeInfo {
	path := []*NodeInfo{}
	if ntarget == nil || ntarget.parent == nil {
		return path
	}

	// Build the path
	ntarget = ntarget.parent

	for ntarget.parent != nil {
		path = append(path, &NodeInfo{ntarget.Name, ntarget.Hash})
		ntarget = ntarget.parent
	}
	path = append(path, &NodeInfo{rootNode.Name, rootNode.Hash})

	// Reverse the slice
	for left, right := 0, len(path)-1; left < right; left, right = left+1, right-1 {
		path[left], path[right] = path[right], path[left]
	}
	return path
}

func nodePath(n, r *Node) string {
	nis := pathFromNode(n, r)
	p := []string{}
	for _, ni := range nis {
		if ni.Name == "_root" {
			continue
		}
		p = append(p, ni.Name)
	}
	if n.Name == "_root" {
		return "/"
	}

	return "/" + strings.Join(append(p, n.Name), "/")
}

// BruteforcePath builds the path from a children hash/ref
func (ft *FileTree) BruteforcePath(ctx context.Context, root, target string) ([]*NodeInfo, error) {
	rootNode, err := ft.nodeByRef(ctx, root)
	if err != nil {
		return nil, err
	}

	// Find the node by performing a BFS search
	ntarget, err := ft.BFS(ctx, rootNode, func(n *Node) (bool, error) {
		return n.Hash == target, nil
	})
	if err != nil {
		return nil, err
	}

	return pathFromNode(ntarget, rootNode), nil
}

func (ft *FileTree) NodeWithChildren(ctx context.Context, hash string) (*Node, error) {
	node, err := ft.nodeByRef(ctx, hash)
	if err != nil {
		return nil, err
	}
	if err := ft.fetchDir(ctx, node, 1, 1); err != nil {
		return nil, err
	}

	// FIXME(tsileo): init the new file in fetchInfo and only if needed
	f := filereader.NewFile(ctx, ft.blobStore, node.Meta, nil)
	defer f.Close()

	info, err := ft.fetchInfo(f, node.Meta.Name, node.Meta.Hash, node.Meta.ContentHash)
	if err != nil {
		panic(err)
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
