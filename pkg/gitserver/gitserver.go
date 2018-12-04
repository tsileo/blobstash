package gitserver // import "a4.io/blobstash/pkg/gitserver"

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	"github.com/restic/chunker"
	"github.com/vmihailenco/msgpack"
	git "gopkg.in/src-d/go-git.v4"
	gconfig "gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	gindex "gopkg.in/src-d/go-git.v4/plumbing/format/index"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/protocol/packp"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	"gopkg.in/src-d/go-git.v4/plumbing/transport/server"
	gstorage "gopkg.in/src-d/go-git.v4/storage"

	"a4.io/blobstash/pkg/auth"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/filetree/writer"
	"a4.io/blobstash/pkg/hashutil"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/hub"
	"a4.io/blobstash/pkg/perms"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
)

var remoteMaster = "refs/remotes/origin/master"

type GitServer struct {
	kvStore   store.KvStore
	blobStore store.BlobStore

	conf *config.Config

	hub *hub.Hub

	log log.Logger
}

// New initializes the `DocStoreExt`
func New(logger log.Logger, conf *config.Config, kvStore store.KvStore, blobStore store.BlobStore, chub *hub.Hub) (*GitServer, error) {
	logger.Debug("init")
	return &GitServer{
		conf:      conf,
		kvStore:   kvStore,
		blobStore: blobStore,
		hub:       chub,
		log:       logger,
	}, nil
}

// Close closes all the open DB files.
func (gs *GitServer) Close() error {
	return nil
}

// RegisterRoute registers all the HTTP handlers for the extension
func (gs *GitServer) Register(r *mux.Router, root *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/", basicAuth(http.HandlerFunc(gs.rootHandler)))
	r.Handle("/{ns}", basicAuth(http.HandlerFunc(gs.nsHandler)))
	r.Handle("/{ns}/{repo}/config", basicAuth(http.HandlerFunc(gs.gitRepoConfigHandler)))
	r.Handle("/{ns}/{repo}/_backup", basicAuth(http.HandlerFunc(gs.gitCloneOrPullHandler)))
	r.Handle("/{ns}/{repo}/_tgz", basicAuth(http.HandlerFunc(gs.gitRepoTgzHandler)))
	r.Handle("/{ns}/{repo}", basicAuth(http.HandlerFunc(gs.gitRepoHandler)))
	root.Handle("/git/{ns}/{repo}.git/info/refs", basicAuth(http.HandlerFunc(gs.gitInfoRefsHandler)))
	root.Handle("/git/{ns}/{repo}.git/{service}", basicAuth(http.HandlerFunc(gs.gitServiceHandler)))
}

type storage struct {
	ns, name  string
	kvStore   store.KvStore
	blobStore store.BlobStore
	cloneMode bool
	tMode     bool
	chunker   *chunker.Chunker
	buf       []byte
}

func newStorage(ns, name string, blobStore store.BlobStore, kvStore store.KvStore) *storage {
	return &storage{
		ns:        ns,
		name:      name,
		kvStore:   kvStore,
		blobStore: blobStore,
		chunker:   chunker.New(bytes.NewReader(nil), writer.Pol),
		buf:       make([]byte, 8*1024*1024),
	}
}

func (s *storage) Load(ep *transport.Endpoint) (storer.Storer, error) {
	fmt.Printf("ep=%+v\n", ep)
	return s, nil
}

func rewriteKey(key string) string {
	return strings.Replace(key, "~", "/", -1)
}

func (s *storage) key(prefix, key string) string {
	// `/` is an illegal character for a key in the kvstore, and `~` is an illegal character for git branches
	key = strings.Replace(key, "/", "~", -1)
	return fmt.Sprintf("_git:%s:%s!%s!%s", s.ns, s.name, prefix, key)
}

func (s *storage) Module(n string) (gstorage.Storer, error) {
	return nil, nil
}

func (s *storage) SetShallow(hashes []plumbing.Hash) error {
	panic("should never happen")
}

func (s *storage) Shallow() ([]plumbing.Hash, error) {
	return []plumbing.Hash{}, nil
}

func (s *storage) SetIndex(idx *gindex.Index) error {
	panic("should never happen")
}

func (s *storage) Index() (*gindex.Index, error) {
	panic("should never happen")
}

func (s *storage) Config() (*gconfig.Config, error) {
	conf := gconfig.NewConfig()
	kv, err := s.kvStore.Get(context.TODO(), s.key("c", "conf"), -1)
	if err != nil {
		if err == vkv.ErrNotFound {
			return conf, nil
		}
		return nil, err
	}
	if kv != nil {
		if err := conf.Unmarshal(kv.Data); err != nil {
			return nil, err
		}
	}

	return conf, nil
}

func (s *storage) SetConfig(c *gconfig.Config) error {
	encoded, err := c.Marshal()
	if err != nil {
		return err
	}
	if _, err := s.kvStore.Put(context.TODO(), s.key("c", "conf"), "", encoded, -1); err != nil {
		return err
	}
	return nil
}

// SetReference implements the storer.ReferenceStorer interface
func (s *storage) SetReference(ref *plumbing.Reference) error {
	parts := ref.Strings()
	if _, err := s.kvStore.Put(context.TODO(), s.key("r", ref.Name().String()), "", []byte(parts[1]), -1); err != nil {
		return err
	}
	// If we're updating the remote master (during a fetch)
	if ref.Name().String() == remoteMaster {
		// Also update the local master/HEAD
		if _, err := s.kvStore.Put(context.TODO(), s.key("r", plumbing.Master.String()), "", []byte(parts[1]), -1); err != nil {
			return err
		}
	}
	return nil
}

// CheckAndSetReference implements the storer.ReferenceStorer interface
func (s *storage) CheckAndSetReference(new, old *plumbing.Reference) error {
	return s.SetReference(new)
}

func (s *storage) RemoveReference(n plumbing.ReferenceName) error {
	if _, err := s.kvStore.Put(context.TODO(), s.key("r", n.String()), "", nil, -1); err != nil {
		return err
	}
	return nil
}

func (s *storage) Reference(name plumbing.ReferenceName) (*plumbing.Reference, error) {
	if !s.tMode && name == plumbing.HEAD {
		return plumbing.NewSymbolicReference(
			plumbing.HEAD,
			plumbing.Master,
		), nil
	}
	kv, err := s.kvStore.Get(context.TODO(), s.key("r", name.String()), -1)
	if err != nil {
		if err == vkv.ErrNotFound {
			return nil, plumbing.ErrReferenceNotFound
		}
		return nil, err
	}
	if kv == nil && kv.Data == nil || len(kv.Data) == 0 {
		// Check if the reference has been removed
		return nil, plumbing.ErrReferenceNotFound

	}
	ref := plumbing.NewReferenceFromStrings(name.String(), string(kv.Data))
	return ref, nil
}

func (s *storage) IterReferences() (storer.ReferenceIter, error) {
	refs := []*plumbing.Reference{}

	rawRefs, _, err := s.kvStore.Keys(context.TODO(), s.key("r", ""), s.key("r", "\xff"), -1)
	if err != nil {
		return nil, err
	}
	for _, kv := range rawRefs {
		refs = append(refs, plumbing.NewReferenceFromStrings(rewriteKey(strings.Replace(kv.Key, s.key("r", ""), "", 1)), string(kv.Data)))
	}

	return storer.NewReferenceSliceIter(refs), nil
}

func (s *storage) CountLooseRefs() (int, error) {
	rawRefs, _, err := s.kvStore.Keys(context.TODO(), s.key("r", ""), s.key("r", "\xff"), -1)
	if err != nil {
		return 0, err
	}
	return len(rawRefs), nil
}

func (s *storage) PackRefs() error {
	return fmt.Errorf("should not happen")
}

// storer.EncodedObjectStorer interface
func (s *storage) NewEncodedObject() plumbing.EncodedObject {
	return &plumbing.MemoryObject{}
}

func (s *storage) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	key := s.key("o", obj.Hash().String())

	reader, err := obj.Reader()
	if err != nil {
		return plumbing.ZeroHash, err
	}

	obj.Size()

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return plumbing.ZeroHash, err
	}

	// Chunk the file the same way the filetree API does to share the "dedup"'d data
	if obj.Type() == plumbing.BlobObject {
		// reuse this buffer
		refs := [][32]byte{}
		if obj.Size() > 512*1024 {
			s.chunker.Reset(bytes.NewReader(content), writer.Pol)
			chunkSplitter := s.chunker
			for {
				chunk, err := chunkSplitter.Next(s.buf)
				if err == io.EOF {
					break
				}
				chunkHash := hashutil.ComputeRaw(chunk.Data)
				if err := s.blobStore.Put(context.TODO(), &blob.Blob{Hash: fmt.Sprintf("%x", chunkHash), Data: chunk.Data}); err != nil {
					return plumbing.ZeroHash, err
				}
				refs = append(refs, chunkHash)
			}
		} else {
			chunkHash := hashutil.ComputeRaw(content)
			if err := s.blobStore.Put(context.TODO(), &blob.Blob{Hash: fmt.Sprintf("%x", chunkHash), Data: content}); err != nil {
				return plumbing.ZeroHash, err
			}
			refs = append(refs, chunkHash)
		}
		content, err = msgpack.Marshal(&refs)
		if err != nil {
			return plumbing.ZeroHash, err
		}
	}

	if _, err := s.kvStore.Put(context.TODO(), key, "", append([]byte{byte(obj.Type())}, content...), -1); err != nil {
		return plumbing.ZeroHash, err

	}

	return obj.Hash(), nil
}

func (s *storage) objFromKv(kv *vkv.KeyValue) (plumbing.EncodedObject, error) {
	obj := &plumbing.MemoryObject{}
	objType := plumbing.ObjectType(kv.Data[0])
	obj.SetType(objType)

	if objType == plumbing.BlobObject {
		refs := [][32]byte{}
		if err := msgpack.Unmarshal(kv.Data[1:], &refs); err != nil {
			return nil, err
		}
		for _, rref := range refs {
			blob, err := s.blobStore.Get(context.TODO(), fmt.Sprintf("%x", rref))
			if err != nil {
				return nil, err
			}
			if _, err := obj.Write(blob); err != nil {
				return nil, err
			}
		}

	} else {
		if _, err := obj.Write(kv.Data[1:]); err != nil {
			return nil, err
		}
	}

	return obj, nil
}

func (s *storage) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	key := s.key("o", h.String())

	kv, err := s.kvStore.Get(context.TODO(), key, -1)
	if err != nil {
		if err == vkv.ErrNotFound {
			return nil, plumbing.ErrObjectNotFound
		}
		return nil, err
	}
	return s.objFromKv(kv)
}

func (s *storage) EncodedObjectSize(h plumbing.Hash) (size int64, err error) {
	key := s.key("o", h.String())

	kv, err := s.kvStore.Get(context.TODO(), key, -1)
	if err != nil {
		if err == vkv.ErrNotFound {
			return 0, plumbing.ErrObjectNotFound
		}
		return 0, err
	}
	obj, err := s.objFromKv(kv)
	if err != nil {
		return 0, err
	}

	return obj.Size(), nil
}

func (s *storage) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	res := []plumbing.EncodedObject{}
	kvs, _, err := s.kvStore.Keys(context.TODO(), s.key("o", ""), s.key("o", "\xff"), -1)
	if err != nil {
		return nil, err
	}

	for _, kv := range kvs {
		if plumbing.ObjectType(kv.Data[0]) != t {
			continue
		}
		obj, err := s.objFromKv(kv)
		if err != nil {
			return nil, err
		}
		res = append(res, obj)
	}

	return storer.NewEncodedObjectSliceIter(res), nil
}

func (s *storage) HasEncodedObject(h plumbing.Hash) error {
	key := s.key("o", h.String())
	switch _, err := s.kvStore.Get(context.TODO(), key, -1); err {
	case nil:
		return nil
	case vkv.ErrNotFound:
		return plumbing.ErrObjectNotFound
	default:
		return err
	}
}

func (gs *GitServer) getEndpoint(path string) (*transport.Endpoint, error) {
	var u string
	if gs.conf.AutoTLS {
		u = fmt.Sprintf("https://%s%s", gs.conf.Domains[0], path)
	} else {
		p, err := url.Parse(fmt.Sprintf("http://%s", gs.conf.Listen))
		if err != nil {
			return nil, err
		}
		hostname := p.Hostname()
		if hostname == "" {
			hostname = "localhost"
		}
		u = fmt.Sprintf("http://%s:%s%s", hostname, p.Port(), path)
	}
	ep, err := transport.NewEndpoint(u)
	if err != nil {
		return nil, err
	}
	return ep, nil
}

type LogBuilder struct {
	commits []*object.Commit
}

func (b *LogBuilder) process(c *object.Commit) error {
	b.commits = append(b.commits, c)
	parents := c.Parents()
	defer parents.Close()
	return parents.ForEach(b.process)
}

func buildCommitLogs(s *storage, h plumbing.Hash) []*object.Commit {
	commit, err := object.GetCommit(s, h)
	//obj, err := storage.EncodedObject(plumbing.CommitObject, ref.Hash())
	if err != nil {
		panic(err)
	}
	lb := &LogBuilder{[]*object.Commit{commit}}
	parents := commit.Parents()
	defer parents.Close()
	if err := parents.ForEach(lb.process); err != nil {
		panic(err)
	}
	return lb.commits
}

func (gs *GitServer) rootHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if !auth.Can(
		w,
		r,
		perms.Action(perms.List, perms.GitNs),
		perms.ResourceWithID(perms.GitServer, perms.GitNs, "*"),
	) {
		auth.Forbidden(w)
		return
	}

	limit := 50

	namespaces, err := gs.Namespaces()
	if err != nil {
		panic(err)
	}

	httputil.MarshalAndWrite(r, w, map[string]interface{}{
		"data": namespaces,
		"pagination": map[string]interface{}{
			"cursor":   "",
			"has_more": len(namespaces) == limit,
			"count":    len(namespaces),
			"per_page": limit,
		},
	})
}

func (gs *GitServer) Namespaces() ([]string, error) {
	namespaces := []string{}

	// We cannot afford to index the repository (will waste space to keep a separate
	// kv collection) and having a temp index is complicated
	prefix := "_git:"
	for {
		keys, _, err := gs.kvStore.Keys(context.TODO(), prefix, "\xff", 1)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 || !strings.HasPrefix(keys[0].Key, prefix) {
			break
		}
		dat := strings.Split(strings.Split(keys[0].Key, "!")[0], ":")
		namespaces = append(namespaces, dat[1])
		prefix = vkv.NextKey(fmt.Sprintf("_git:%s:", dat[1]))
	}

	return namespaces, nil
}

func (gs *GitServer) Repositories(ns string) ([]string, error) {
	repos := []string{}
	// We cannot afford to index the repository (will waste space to keep a separate
	// kv collection) and having a temp index is complicated
	basePrefix := fmt.Sprintf("_git:%s:", ns)
	prefix := fmt.Sprintf("_git:%s:", ns)
	for {
		keys, _, err := gs.kvStore.Keys(context.TODO(), prefix, "\xff", 1)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 || !strings.HasPrefix(keys[0].Key, basePrefix) {
			break
		}
		repo := strings.Split(keys[0].Key, "!")[0]
		repo = repo[len(basePrefix):]
		repos = append(repos, repo)
		prefix = vkv.NextKey(fmt.Sprintf("_git:%s:%s", ns, repo))
	}
	return repos, nil
}

func (gs *GitServer) nsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)

	limit := 50

	ns := vars["ns"]

	if !auth.Can(
		w,
		r,
		perms.Action(perms.List, perms.GitNs),
		perms.ResourceWithID(perms.GitServer, perms.GitNs, ns),
	) {
		auth.Forbidden(w)
		return
	}

	repos, err := gs.Repositories(ns)
	if err != nil {
		panic(err)
	}

	httputil.MarshalAndWrite(r, w, map[string]interface{}{
		"data": repos,
		"pagination": map[string]interface{}{
			"cursor":   "",
			"has_more": len(repos) == limit,
			"count":    len(repos),
			"per_page": limit,
		},
	})
}

type GitRepoRefs struct {
	Branches []*RefSummary
	Tags     []*RefSummary
}

type GitRepoSummary struct {
	CommitsCount int
	Commits      []*object.Commit
	Readme       *object.File
}

type RefSummary struct {
	Ref    *plumbing.Reference
	Commit *object.Commit
}

func (gs *GitServer) RepoGetFile(ns, repo string, hash plumbing.Hash) (*object.File, error) {
	storage := newStorage(ns, repo, gs.blobStore, gs.kvStore)

	blob, err := object.GetBlob(storage, hash)
	if err != nil {
		return nil, err
	}

	return object.NewFile(hash.String(), 0644, blob), nil
}

func (gs *GitServer) RepoGetTree(ns, repo, hash string) (*object.Tree, error) {
	storage := newStorage(ns, repo, gs.blobStore, gs.kvStore)
	tree, err := object.GetTree(storage, plumbing.NewHash(hash))
	if err != nil {
		return nil, err
	}
	return tree, nil
}

func (gs *GitServer) RepoTree(ns, repo string) (*object.Tree, error) {
	storage := newStorage(ns, repo, gs.blobStore, gs.kvStore)
	ref, err := storage.Reference(plumbing.Master)
	if err != nil {
		return nil, err
	}
	commit, err := object.GetCommit(storage, ref.Hash())
	if err != nil {
		return nil, err
	}
	return commit.Tree()
}

func (gs *GitServer) RepoLog(ns, repo string) ([]*object.Commit, error) {
	storage := newStorage(ns, repo, gs.blobStore, gs.kvStore)
	ref, err := storage.Reference(plumbing.Master)
	if err != nil {
		return nil, err
	}
	commits := buildCommitLogs(storage, ref.Hash())
	return commits, nil
}

func (gs *GitServer) RepoCommit(ns, repo string, hash plumbing.Hash) (*object.Commit, error) {
	storage := newStorage(ns, repo, gs.blobStore, gs.kvStore)
	commit, err := object.GetCommit(storage, hash)
	if err != nil {
		panic(err)
	}
	return commit, nil
}

func (gs *GitServer) RepoRefs(ns, repo string) (*GitRepoRefs, error) {
	summary := &GitRepoRefs{}

	storage := newStorage(ns, repo, gs.blobStore, gs.kvStore)
	refs, err := storage.IterReferences()
	if err != nil {
		return nil, err
	}
	if err := refs.ForEach(func(ref *plumbing.Reference) error {
		if ref.Name().IsTag() {
			commit, err := object.GetCommit(storage, ref.Hash())
			if err != nil {
				return fmt.Errorf("failed to fetch tag: %+v", err)
			}
			summary.Tags = append(summary.Tags, &RefSummary{Ref: ref, Commit: commit})
		}
		if ref.Name().IsBranch() {
			commit, err := object.GetCommit(storage, ref.Hash())
			if err != nil {
				return err
			}
			summary.Branches = append(summary.Branches, &RefSummary{Ref: ref, Commit: commit})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return summary, nil
}

type gitServerBranch struct {
	Name   string `json:"name"`
	Remote string `json:"remote"`
	Merge  string `json:"merge"`
}

type gitServerRemote struct {
	Name  string   `json:"name"`
	URLs  []string `json:"urls"`
	Fetch []string `json:"fetch"`
}

type gitServerConfig struct {
	Branches map[string]*gitServerBranch `json:"branches"`
	Remotes  map[string]*gitServerRemote `json:"remotes"`
}

func (gs *GitServer) Config(ns, repo string) (*gitServerConfig, error) {
	storage := newStorage(ns, repo, gs.blobStore, gs.kvStore)
	conf, err := storage.Config()
	if err != nil {
		return nil, err
	}

	rconf := &gitServerConfig{
		Branches: map[string]*gitServerBranch{},
		Remotes:  map[string]*gitServerRemote{},
	}
	// TODO(tsileo): submodule support?

	for name, gbranch := range conf.Branches {
		rconf.Branches[name] = &gitServerBranch{
			Name:   name,
			Remote: gbranch.Remote,
			Merge:  gbranch.Merge.String(),
		}
	}
	for name, gremote := range conf.Remotes {
		fetch := []string{}
		for _, k := range gremote.Fetch {
			fetch = append(fetch, k.String())
		}
		rconf.Remotes[name] = &gitServerRemote{
			Name:  name,
			URLs:  gremote.URLs,
			Fetch: fetch,
		}
	}

	return rconf, nil
}

func (gs *GitServer) RepoSummary(ns, repo string) (*GitRepoSummary, error) {
	summary := &GitRepoSummary{}

	storage := newStorage(ns, repo, gs.blobStore, gs.kvStore)

	ref, err := storage.Reference(plumbing.Master)
	if err != nil {
		return nil, err
	}
	commit, err := object.GetCommit(storage, ref.Hash())
	if err != nil {
		panic(err)
	}
	tree, err := commit.Tree()
	if err != nil {
		panic(err)
	}
	for _, treeEntry := range tree.Entries {
		if strings.HasSuffix(treeEntry.Name, "README.md") ||
			strings.HasSuffix(treeEntry.Name, "README.rst") ||
			strings.HasSuffix(treeEntry.Name, "README.txt") ||
			strings.HasSuffix(treeEntry.Name, "README") {
			f, err := tree.File(treeEntry.Name)
			if err != nil {
				panic(err)
			}
			summary.Readme = f
		}
	}

	commits := buildCommitLogs(storage, ref.Hash())
	summary.CommitsCount = len(commits)
	summary.Commits = commits[0:3]
	return summary, nil
}

func (gs *GitServer) gitRepoTgzHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)

	ns := vars["ns"]

	if !auth.Can(
		w,
		r,
		perms.Action(perms.Read, perms.GitRepo),
		perms.ResourceWithID(perms.GitServer, perms.GitRepo, fmt.Sprintf("%s/%s", ns, vars["repo"])),
	) {
		auth.Forbidden(w)
		return
	}

	tree, err := gs.RepoTree(vars["ns"], vars["repo"])
	switch err {
	case nil:
	case plumbing.ErrReferenceNotFound:
		w.WriteHeader(http.StatusNotFound)
		return
	default:
		panic(err)
	}

	gzipWriter := gzip.NewWriter(w)
	tarWriter := tar.NewWriter(gzipWriter)

	// Iter the whole tree
	fiter := tree.Files()
	if err := fiter.ForEach(func(o *object.File) error {
		// Write the tar header
		hdr := &tar.Header{
			Name: filepath.Join(vars["repo"], o.Name),
			Mode: int64(o.Mode),
			Size: o.Size,
		}
		if err := tarWriter.WriteHeader(hdr); err != nil {
			return err
		}

		r, err := o.Reader()
		if err != nil {
			return err
		}
		defer r.Close()

		if _, err := io.Copy(tarWriter, r); err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}

	// "seal" the tarfile
	tarWriter.Close()
	gzipWriter.Close()
}

func (gs *GitServer) gitRepoConfigHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)

	ns := vars["ns"]

	if !auth.Can(
		w,
		r,
		perms.Action(perms.Read, perms.GitRepo),
		perms.ResourceWithID(perms.GitServer, perms.GitRepo, fmt.Sprintf("%s/%s", ns, vars["repo"])),
	) {
		auth.Forbidden(w)
		return
	}

	conf, err := gs.Config(vars["ns"], vars["repo"])
	switch err {
	case nil:
	case plumbing.ErrReferenceNotFound:
		w.WriteHeader(http.StatusNotFound)
		return
	default:
		panic(err)
	}
	httputil.MarshalAndWrite(r, w, map[string]interface{}{
		"data": map[string]interface{}{
			"ns":         ns,
			"repository": vars["repo"],
			"config":     conf,
		},
	})

}

func (gs *GitServer) gitRepoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)

	ns := vars["ns"]

	if !auth.Can(
		w,
		r,
		perms.Action(perms.Read, perms.GitRepo),
		perms.ResourceWithID(perms.GitServer, perms.GitRepo, fmt.Sprintf("%s/%s", ns, vars["repo"])),
	) {
		auth.Forbidden(w)
		return
	}

	summary := &GitRepoSummary{}

	storage := newStorage(vars["ns"], vars["repo"], gs.blobStore, gs.kvStore)
	ref, err := storage.Reference(plumbing.Master)
	switch err {
	case nil:
	case plumbing.ErrReferenceNotFound:
		w.WriteHeader(http.StatusNotFound)
		return
	default:
		panic(err)
	}
	commits := buildCommitLogs(storage, ref.Hash())
	summary.Commits = commits
	//reader, _ := obj.Reader()
	//data, err := ioutil.ReadAll(reader)
	//if err != nil {
	//	panic(err)
	//}
	httputil.MarshalAndWrite(r, w, map[string]interface{}{
		"data": map[string]interface{}{
			"ns":         ns,
			"repository": vars["repo"],
			// "commits":    commits,
		},
	})

}

func (gs *GitServer) gitInfoRefsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)

	service := r.URL.Query().Get("service")

	// Compute the permission for the RBAC (default to Write)
	perm := perms.Write
	if service == "git-upload-pack" {
		// If it's a `git clone`, set the permission to Read
		perm = perms.Read
	}

	// Check the perms
	if !auth.Can(
		w,
		r,
		perms.Action(perm, perms.GitRepo),
		perms.ResourceWithID(perms.GitServer, perms.GitRepo, fmt.Sprintf("%s/%s", vars["ns"], vars["repo"])),
	) {
		auth.Forbidden(w)
		return
	}

	var refs *packp.AdvRefs

	// Here, repositories are created on the fly, we don't need to check if it actually exists before
	storage := newStorage(vars["ns"], vars["repo"], gs.blobStore, gs.kvStore)
	git := server.NewServer(storage)
	t, err := gs.getEndpoint(r.URL.Path)
	if err != nil {
		panic(err)
	}

	switch service {
	case "git-upload-pack":
		sess, err := git.NewUploadPackSession(t, nil)
		if err != nil {
			panic(err)
		}
		refs, err = sess.AdvertisedReferences()
		if err != nil {
			panic(err)
		}
		fmt.Printf("refs=%+v\n", refs)
	case "git-receive-pack":
		sess, err := git.NewReceivePackSession(t, nil)
		if err != nil {
			panic(err)
		}
		refs, err = sess.AdvertisedReferences()
		if err != nil {
			panic(err)
		}
	}

	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-advertisement", service))
	w.Header().Set("Cache-Control", "no-cache")

	data := fmt.Sprintf("# service=%s\n0000", service)
	w.Write([]byte(fmt.Sprintf("%04x%s", len(data), data)))
	if err := refs.Encode(w); err != nil {
		panic(err)
	}
}

type cloneReq struct {
	URL string `json:"url"`
}

func (gs *GitServer) gitCloneOrPullHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)

	// Check the perms
	if !auth.Can(
		w,
		r,
		perms.Action(perms.Write, perms.GitRepo),
		perms.ResourceWithID(perms.GitServer, perms.GitRepo, fmt.Sprintf("%s/%s", vars["ns"], vars["repo"])),
	) {
		auth.Forbidden(w)
		return
	}

	// Parse the payload (the remote URL)
	creq := &cloneReq{}
	if err := httputil.Unmarshal(r, creq); err != nil {
		panic(err)
	}

	// Intialize the git backend
	storage := newStorage(vars["ns"], vars["repo"], gs.blobStore, gs.kvStore)
	storage.tMode = true

	httputil.HeaderLog(w, "git clone")

	// Try to clone the repo
	_, err := git.Clone(storage, nil, &git.CloneOptions{
		URL: creq.URL,
	})
	switch err {
	case nil:
		httputil.HeaderLog(w, "clone succeeded")
		w.WriteHeader(http.StatusCreated)
	case git.ErrRepositoryAlreadyExists:
		httputil.HeaderLog(w, "git fetch")

		// If the repo already exists, "open it"
		repo, err := git.Open(storage, nil)
		if err != nil {
			panic(err)
		}

		// Try to fetch the latest change
		switch err := repo.Fetch(&git.FetchOptions{}); err {
		case nil:
			httputil.HeaderLog(w, "fetch succeeded")
			w.WriteHeader(http.StatusResetContent)
		case git.NoErrAlreadyUpToDate:
			httputil.HeaderLog(w, err.Error())
			w.WriteHeader(http.StatusNoContent)
		default:
			panic(err)
		}

	default:
		panic(err)
	}
}

func (gs *GitServer) gitServiceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)
	service := vars["service"]

	// Compute the permission for the RBAC (default to Write)
	perm := perms.Write
	if service == "git-upload-pack" {
		// If it's a `git clone`, set the permission to Read
		perm = perms.Read
	}

	// Check the perms
	if !auth.Can(
		w,
		r,
		perms.Action(perm, perms.GitRepo),
		perms.ResourceWithID(perms.GitServer, perms.GitRepo, fmt.Sprintf("%s/%s", vars["ns"], vars["repo"])),
	) {
		auth.Forbidden(w)
		return
	}

	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-result", service))

	storage := newStorage(vars["ns"], vars["repo"], gs.blobStore, gs.kvStore)
	git := server.NewServer(storage)
	t, err := gs.getEndpoint(r.URL.Path)
	if err != nil {
		panic(err)
	}

	switch service {
	case "git-receive-pack":
		// Handle push
		req := packp.NewReferenceUpdateRequest()
		sess, err := git.NewReceivePackSession(t, nil)
		if err != nil {
			panic(err)
		}

		if err := req.Decode(r.Body); err != nil {
			panic(err)
		}

		status, err := sess.ReceivePack(r.Context(), req)
		if err != nil {
			panic(err)
		}

		if err := status.Encode(w); err != nil {
			panic(err)
		}
	case "git-upload-pack":
		// Handle clone
		req := packp.NewUploadPackRequest()
		sess, err := git.NewUploadPackSession(t, nil)
		if err != nil {
			panic(err)
		}

		if err := req.Decode(r.Body); err != nil {
			panic(err)
		}

		resp, err := sess.UploadPack(r.Context(), req)
		if err != nil {
			panic(err)
		}

		if err := resp.Encode(w); err != nil {
			panic(err)
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}

}
