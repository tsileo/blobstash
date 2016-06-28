package filetree

import (
	_ "encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	"golang.org/x/net/context"
	"io"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/tsileo/blobstash/pkg/blobstore"
	"github.com/tsileo/blobstash/pkg/client/clientutil"
	"github.com/tsileo/blobstash/pkg/config"
	"github.com/tsileo/blobstash/pkg/filetree/filetreeutil/meta"
	"github.com/tsileo/blobstash/pkg/filetree/reader/filereader"
	"github.com/tsileo/blobstash/pkg/httputil"
	"github.com/tsileo/blobstash/pkg/httputil/bewit"
	"github.com/tsileo/blobstash/pkg/httputil/resize"
	"github.com/tsileo/blobstash/pkg/kvstore"
)

// TODO(tsileo): handle the fetching of meta from the FS name and reconstruct the vkv key
// to the root in children link
// TODO(tsileo): bind a folder to the root path, e.g. {hostname}/ => dirHandler?
// TODO(tsileo): a multi-part upload endpoint (but without dir capabilities, at least for now)

var (
	indexFile = "index.html"

	// PermName     = "filetree"
	// PermTreeName = "filetree:root:"
	// PermWrite    = "write"
	// PermRead     = "read"
)

type FileTreeExt struct {
	kvStore     *kvstore.KvStore
	blobStore   *blobstore.BlobStore
	conf        *config.Config
	sharingCred *bewit.Cred
	authFunc    func(*http.Request) bool
	shareTTL    time.Duration

	log log.Logger
}

// New initializes the `DocStoreExt`
func New(logger log.Logger, conf *config.Config, authFunc func(*http.Request) bool, kvStore *kvstore.KvStore, blobStore *blobstore.BlobStore) (*FileTreeExt, error) {
	logger.Debug("init")
	return &FileTreeExt{
		conf:      conf,
		kvStore:   kvStore,
		blobStore: blobStore,
		sharingCred: &bewit.Cred{
			Key: []byte(conf.SharingKey),
			ID:  "filetree",
		},
		authFunc: authFunc,
		shareTTL: 1 * time.Hour,
		log:      logger,
	}, nil
}

// Close closes all the open DB files.
func (ft *FileTreeExt) Close() error {
	return nil
}

// RegisterRoute registers all the HTTP handlers for the extension
func (ft *FileTreeExt) Register(r *mux.Router, root *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/node/{ref}", basicAuth(http.HandlerFunc(ft.nodeHandler())))

	// Public/semi-private handler
	dirHandler := http.HandlerFunc(ft.dirHandler())
	fileHandler := http.HandlerFunc(ft.fileHandler())

	// r.Handle("/fs", http.HandlerFunc(ft.fsHandler()))
	// r.Handle("/fs/{name}", http.HandlerFunc(ft.fsByNameHandler()))

	// Hook the standard endpint
	r.Handle("/dir/{ref}", dirHandler)
	r.Handle("/file/{ref}", fileHandler)

	// Enable shortcut path from the root
	root.Handle("/d/{ref}", dirHandler)
	root.Handle("/f/{ref}", fileHandler)
}

type Node struct {
	Name     string  `json:"name"`
	Type     string  `json:"type"`
	Size     int     `json:"size"`
	Mode     uint32  `json:"mode"`
	ModTime  string  `json:"mtime"`
	Hash     string  `json:"ref"`
	Children []*Node `json:"children,omitempty"`

	Extra  map[string]interface{} `json:"extra,omitempty"`
	XAttrs map[string]string      `json:"xattrs,omitempty"`

	meta *meta.Meta `json:"-"`
}

func (n *Node) Close() error {
	n.meta.Close()
	return nil
}

type byName []*Node

func (s byName) Len() int           { return len(s) }
func (s byName) Less(i, j int) bool { return s[i].Name < s[j].Name }
func (s byName) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func metaToNode(m *meta.Meta) (*Node, error) {
	return &Node{
		Name:    m.Name,
		Type:    m.Type,
		Size:    m.Size,
		Mode:    m.Mode,
		ModTime: m.ModTime,
		Extra:   m.Extra,
		XAttrs:  m.XAttrs,
		Hash:    m.Hash,
		meta:    m,
	}, nil
}

// fetchDir recursively fetch dir children
func (ft *FileTreeExt) fetchDir(n *Node, depth int) error {
	if depth >= 10 {
		return nil
	}
	if n.Type == "dir" {
		n.Children = []*Node{}
		for _, ref := range n.meta.Refs {
			cn, err := ft.nodeByRef(ref.(string))
			if err != nil {
				return err
			}
			n.Children = append(n.Children, cn)
			if err := ft.fetchDir(cn, depth+1); err != nil {
				return err
			}
		}
	}
	n.meta.Close()
	return nil
}

// func (ft *FileTreeExt) loadFS(name string) (*fsmod.FS, error) {
// 	fs := &fsmod.FS{}
// 	kv, err := ft.kvStore.Get(context.TODO(), fmt.Sprintf(fsmod.FSKeyFmt, name), -1)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if err := json.Unmarshal([]byte(kv.Value), fs); err != nil {
// 		return nil, err
// 	}
// 	return fs, nil
// }

// func (ft *FileTreeExt) fsHandler() func(http.ResponseWriter, *http.Request) {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		// Check permissions
// 		// permissions.CheckPerms(r, PermName)

// 		// List available FS, and display them
// 		switch r.Method {
// 		case "GET", "HEAD":
// 			if r.Method == "HEAD" {
// 				return
// 			}
// 			res, err := ft.kvStore.Keys(context.TODO(), "filetree:fs:", "filetree:fs:\xff", 0)
// 			if err != nil {
// 				panic(err)
// 			}
// 			out := []*fsmod.FS{}
// 			for _, kv := range res {
// 				fs := &fsmod.FS{}
// 				if err := json.Unmarshal([]byte(kv.Value), fs); err != nil {
// 					panic(err)
// 				}
// 				out = append(out, fs)
// 			}
// 			httputil.WriteJSON(w, out)
// 		case "POST":
// 			defer r.Body.Close()
// 			fs := &fsmod.FS{}
// 			if err := json.NewDecoder(r.Body).Decode(fs); err != nil {
// 				panic(err)
// 			}
// 			fs.SetDB(ft.kvStore)
// 			if err := fs.Mutate(fs.Ref); err != nil {
// 				panic(err)
// 			}
// 			httputil.WriteJSON(w, fs)
// 		default:
// 			w.WriteHeader(http.StatusMethodNotAllowed)
// 			return
// 		}
// 	}
// }

// func (ft *FileTreeExt) fsByNameHandler() func(http.ResponseWriter, *http.Request) {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		// Check permissions
// 		// permissions.CheckPerms(r, PermName)
// 		vars := mux.Vars(r)

// 		name := vars["name"]
// 		fs, err := ft.loadFS(name)
// 		if err != nil {
// 			panic(err)
// 		}

// 		// Display a single FS and handle mutation via POST
// 		switch r.Method {
// 		case "GET", "HEAD":
// 			if r.Method == "HEAD" {
// 				return
// 			}
// 			httputil.WriteJSON(w, fs)
// 		case "POST":
// 			defer r.Body.Close()
// 			fsUpdate := &fsmod.FS{}
// 			if err := json.NewDecoder(r.Body).Decode(fsUpdate); err != nil {
// 				panic(err)
// 			}
// 			// FIXME(tsileo): handle `Data` field mutation
// 			fs.SetDB(ft.kvStore)
// 			if err := fs.Mutate(fsUpdate.Ref); err != nil {
// 				panic(err)
// 			}

// 		default:
// 			w.WriteHeader(http.StatusMethodNotAllowed)
// 			return
// 		}
// 	}
// }

func (ft *FileTreeExt) fileHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		vars := mux.Vars(r)

		hash := vars["ref"]
		ft.serveFile(w, r, hash)
	}
}

func (ft *FileTreeExt) isPublic(m *meta.Meta) (bool, error) {
	if m.IsPublic() {
		ft.log.Debug("XAttrs public=1")
		return true, nil
	}
	// FIXME(tsileo): a way to find if a meta is part of a public directory
	return false, nil
}

// serveFile serve the node as a file using `net/http` FS util
func (ft *FileTreeExt) serveFile(w http.ResponseWriter, r *http.Request, hash string) {
	var authorized bool

	if err := bewit.Validate(r, ft.sharingCred); err != nil {
		ft.log.Debug("invalid bewit", "err", err)
	} else {
		ft.log.Debug("valid bewit")
		authorized = true
	}

	blob, err := ft.blobStore.Get(context.TODO(), hash)
	if err != nil {
		if err == clientutil.ErrBlobNotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		panic(err)
	}

	m, err := meta.NewMetaFromBlob(hash, blob)
	if err != nil {
		panic(err)
	}
	defer m.Close()

	if !authorized && m.IsPublic() {
		ft.log.Debug("XAttrs public=1")
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

	if m.IsDir() {
		panic(httputil.NewPublicErrorFmt("node is not a file (%s)", m.Type))
	}

	// Initialize a new `File`
	var f io.ReadSeeker
	f = filereader.NewFile(ft.blobStore, m)

	// Check if the file is requested for download (?dl=1)
	httputil.SetAttachment(m.Name, r, w)

	// Support for resizing image on the fly
	if err := resize.Resize(m.Name, f, r); err != nil {
		panic(err)
	}

	// Serve the file content using the same code as the `http.ServeFile` (it'll handle HEAD request)
	mtime, _ := m.Mtime()
	http.ServeContent(w, r, m.Name, mtime, f)
}

func (ft *FileTreeExt) nodeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check permissions
		// permissions.CheckPerms(r, PermName)

		// TODO(tsileo): limit the max depth of the tree configurable via query args
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		vars := mux.Vars(r)

		hash := vars["ref"]
		n, err := ft.nodeByRef(hash)
		if err != nil {
			if err == clientutil.ErrBlobNotFound {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			panic(err)
		}

		// Output some headers about ACLs
		u := &url.URL{Path: fmt.Sprintf("/%s/%s", n.Type[0:1], n.Hash)}
		pubHeader := "0"
		if n.meta.IsPublic() {
			pubHeader = "1"
			w.Header().Add("BlobStash-FileTree-Public-Path", u.String())
		}
		w.Header().Add("BlobStash-FileTree-Public", pubHeader)

		if r.URL.Query().Get("bewit") == "1" {
			if err := bewit.Bewit(ft.sharingCred, u, ft.shareTTL); err != nil {
				panic(err)
			}
			w.Header().Add("BlobStash-FileTree-SemiPrivate-Path", u.String())
			w.Header().Add("BlobStash-FileTree-Bewit", u.Query().Get("bewit"))
		}

		if r.Method == "HEAD" {
			return
		}

		if err := ft.fetchDir(n, 1); err != nil {
			panic(err)
		}

		httputil.WriteJSON(w, map[string]interface{}{
			"node": n,
		})
	}
}

// nodeByRef fetch the blob containing the `meta.Meta` and convert it to a `Node`
func (ft *FileTreeExt) nodeByRef(hash string) (*Node, error) {
	blob, err := ft.blobStore.Get(context.TODO(), hash)
	if err != nil {
		return nil, err
	}

	m, err := meta.NewMetaFromBlob(hash, blob)
	if err != nil {
		return nil, err
	}

	n, err := metaToNode(m)
	if err != nil {
		return nil, err
	}

	return n, nil
}

func notFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, "<!doctype html><title>BlobStash</title><p>%s</p>\n", http.StatusText(http.StatusNotFound))

}

func (ft *FileTreeExt) dirHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var authorized bool

		if err := bewit.Validate(r, ft.sharingCred); err != nil {
			ft.log.Debug("invalid bewit", "err", err)
		} else {
			ft.log.Debug("valid bewit")
			authorized = true
		}

		vars := mux.Vars(r)

		hash := vars["ref"]
		n, err := ft.nodeByRef(hash)
		if err != nil {
			if err == clientutil.ErrBlobNotFound {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			panic(err)
		}

		if !authorized && n.meta.IsPublic() {
			ft.log.Debug("XAttrs public=1")
			authorized = true
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

		if err := ft.fetchDir(n, 1); err != nil {
			panic(err)
		}

		sort.Sort(byName(n.Children))

		// Check if the dir contains an "index.html")
		for _, cn := range n.Children {
			if cn.Name == indexFile {
				ft.serveFile(w, r, cn.Hash)
				return
			}
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, "<!doctype html><title>BlobStash - %s</title><pre>\n", n.Name)
		for _, cn := range n.Children {
			u := &url.URL{Path: fmt.Sprintf("/%s/%s", cn.Type[0:1], cn.Hash)}

			// Only compute the Bewit if the node is not public
			if !cn.meta.IsPublic() {
				if err := bewit.Bewit(ft.sharingCred, u, ft.shareTTL); err != nil {
					panic(err)
				}
			}
			fmt.Fprintf(w, "<a href=\"%s\">%s</a>\n", u.String(), cn.Name)
		}
		fmt.Fprintf(w, "</pre>\n")
	}
}
