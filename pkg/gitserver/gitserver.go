package gitserver // import "a4.io/blobstash/pkg/gitserver"

import (
	"bytes"
	"context"
	"crypto/subtle"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	"github.com/restic/chunker"
	"github.com/vmihailenco/msgpack"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/protocol/packp"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	"gopkg.in/src-d/go-git.v4/plumbing/transport/server"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/filetree/writer"
	"a4.io/blobstash/pkg/hashutil"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/hub"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
)

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

func (gs *GitServer) checkNamespace(w http.ResponseWriter, r *http.Request, ns string) bool {
	if gs.conf.GitServer == nil {
		return false
	}
	conf := gs.conf.GitServer.Namespaces[ns]
	if conf == nil {
		w.WriteHeader(http.StatusNotFound)
		return false
	}
	user, pass, ok := r.BasicAuth()

	if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(conf.Username)) != 1 || subtle.ConstantTimeCompare([]byte(pass), []byte(conf.Password)) != 1 {
		w.Header().Set("WWW-Authenticate", `Basic realm="BlobStash git server"`)
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(http.StatusText(http.StatusUnauthorized)))
		return false
	}
	return true
}

// RegisterRoute registers all the HTTP handlers for the extension
func (gs *GitServer) Register(r *mux.Router, root *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/", basicAuth(http.HandlerFunc(gs.rootHandler)))
	r.Handle("/{ns}", basicAuth(http.HandlerFunc(gs.nsHandler)))
	r.Handle("/{ns}/{repo}.git", basicAuth(http.HandlerFunc(gs.gitRepoHandler)))
	root.Handle("/git/{ns}/{repo}.git/info/refs", http.HandlerFunc(gs.gitInfoRefsHandler))
	root.Handle("/git/{ns}/{repo}.git/{service}", http.HandlerFunc(gs.gitServiceHandler))
}

type storage struct {
	ns, name  string
	kvStore   store.KvStore
	blobStore store.BlobStore
}

func newStorage(ns, name string, blobStore store.BlobStore, kvStore store.KvStore) *storage {
	return &storage{
		ns:        ns,
		name:      name,
		kvStore:   kvStore,
		blobStore: blobStore,
	}
}

func (s *storage) Load(ep *transport.Endpoint) (storer.Storer, error) {
	fmt.Printf("ep=%+v\n", ep)
	return s, nil
}

func (s *storage) key(prefix, key string) string {
	return fmt.Sprintf("_git:%s:%s/%s/%s", s.ns, s.name, prefix, key)
}

// SetReference implements the storer.ReferenceStorer interface
func (s *storage) SetReference(ref *plumbing.Reference) error {
	parts := ref.Strings()
	if _, err := s.kvStore.Put(context.TODO(), s.key("r", ref.Name().String()), "", []byte(parts[1]), -1); err != nil {
		return err
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
	fmt.Printf("ref=%+v\n", name)
	if name == plumbing.HEAD {
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
	if kv.Data == nil || len(kv.Data) == 0 {
		// Check if the reference has been removed
		return nil, plumbing.ErrReferenceNotFound

	}
	return plumbing.NewReferenceFromStrings(name.String(), string(kv.Data)), nil
}

func (s *storage) IterReferences() (storer.ReferenceIter, error) {
	refs := []*plumbing.Reference{}

	rawRefs, _, err := s.kvStore.Keys(context.TODO(), s.key("r", ""), s.key("r", "\xff"), -1)
	if err != nil {
		return nil, err
	}
	for _, kv := range rawRefs {
		refs = append(refs, plumbing.NewReferenceFromStrings(strings.Replace(kv.Key, s.key("r", ""), "", 1), string(kv.Data)))
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
			buf := make([]byte, 8*1024*1024)
			chunkSplitter := chunker.New(bytes.NewReader(content), writer.Pol)
			for {
				chunk, err := chunkSplitter.Next(buf)
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
		return nil, err
	}
	return s.objFromKv(kv)
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
	_, err := s.kvStore.Get(context.TODO(), key, -1)
	return err
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
	fmt.Printf("endpoint=%s\n", u)
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

	// FIXME(tsileo): list namespaces from the kvstore `_git:` like in the nsHandler

	for ns, _ := range gs.conf.GitServer.Namespaces {
		namespaces = append(namespaces, ns)
	}
	return namespaces, nil
}

func (gs *GitServer) Repositories(ns string) ([]string, error) {
	repos := []string{}
	// We cannot afford to index the repository (will waste space to keep a separate
	// kv collection) and having a temp index is complicated
	prefix := fmt.Sprintf("_git:%s:", ns)
	for {
		keys, _, err := gs.kvStore.Keys(context.TODO(), prefix, "\xff", 1)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 || !strings.HasPrefix(keys[0].Key, prefix) {
			break
		}
		repo := strings.Split(keys[0].Key, "/")[0]
		repo = repo[len(prefix):]
		repos = append(repos, repo)
		prefix = vkv.NextKey(prefix)
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
	if gs.conf.GitServer == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	conf := gs.conf.GitServer.Namespaces[ns]
	if conf == nil {
		w.WriteHeader(http.StatusNotFound)
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

func (gs *GitServer) gitRepoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)

	ns := vars["ns"]
	if gs.conf.GitServer == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	conf := gs.conf.GitServer.Namespaces[ns]
	if conf == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	summary := &GitRepoSummary{}

	storage := newStorage(vars["ns"], vars["repo"], gs.blobStore, gs.kvStore)
	ref, err := storage.Reference(plumbing.Master)
	if err != nil {
		panic(err)
	}
	commits := buildCommitLogs(storage, ref.Hash())
	summary.Commits = commits
	fmt.Printf("SUMMARY: %+v\n", summary)
	//reader, _ := obj.Reader()
	//data, err := ioutil.ReadAll(reader)
	//if err != nil {
	//	panic(err)
	//}
	fmt.Printf("REF=%+v\nOBJ=%+v\n", ref, commits)
	httputil.MarshalAndWrite(r, w, map[string]interface{}{
		"data": summary,
	})

}

func (gs *GitServer) gitInfoRefsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)

	if ok := gs.checkNamespace(w, r, vars["ns"]); !ok {
		return
	}

	service := r.URL.Query().Get("service")
	var refs *packp.AdvRefs

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

func (gs *GitServer) gitServiceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)

	if ok := gs.checkNamespace(w, r, vars["ns"]); !ok {
		return
	}

	service := vars["service"]
	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-result", service))

	storage := newStorage(vars["ns"], vars["repo"], gs.blobStore, gs.kvStore)
	git := server.NewServer(storage)
	t, err := gs.getEndpoint(r.URL.Path)
	if err != nil {
		panic(err)
	}

	switch service {
	case "git-receive-pack":
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
