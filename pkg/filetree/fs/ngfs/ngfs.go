// Hellofs implements a simple "hello world" file system.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/kvstore"
	"a4.io/blobstash/pkg/config/pathutil"
	"a4.io/blobstash/pkg/ctxutil"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
	"a4.io/blobstash/pkg/filetree/writer"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	yaml "gopkg.in/yaml.v2"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

// Permissions bits for mode manipulation (borrowed from https://github.com/phayes/permbits/blob/master/permbits.go#L10)
const (
	setuid uint32 = 1 << (12 - 1 - iota)
	setgid
	sticky
	userRead
	userWrite
	userExecute
	groupRead
	groupWrite
	groupExecute
	otherRead
	otherWrite
	otherExecute
)

// TODO(tsileo):
// - support file@<date> ; e.g.: file.txt@2017-5-4T21:30 ???
// - `-snapshot` mode that lock to the current version, very efficient, can specify a snapshot version `-at`

type RemoteConfig struct {
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	KeyFile         string `yaml:"key_file"`
}

type Profile struct {
	RemoteConfig *RemoteConfig `yaml:"remote_config"`
	Endpoint     string        `yaml:"endpoint"`
	APIKey       string        `yaml:"api_key"`
}

type Config map[string]*Profile

func loadProfile(configFile, name string) (*Profile, error) {
	dat, err := ioutil.ReadFile(configFile)
	switch {
	case err == nil:
	case os.IsNotExist(err):
		return nil, nil
	default:
		return nil, err
	}
	out := Config{}
	if err := yaml.Unmarshal(dat, out); err != nil {
		return nil, err
	}

	prof, ok := out[name]
	if !ok {
		return nil, fmt.Errorf("profile %s not found", name)
	}

	return prof, nil
}

const revisionHeader = "BlobStash-Filetree-FS-Revision"

func main() {
	// Scans the arg list and sets up flags
	//debug := flag.Bool("debug", false, "print debugging messages.")
	resetCache := flag.Bool("reset-cache", false, "remove the local cache before starting.")
	//roMode := flag.Bool("ro", false, "read-only mode")
	//syncDelay := flag.Duration("sync-delay", 5*time.Minute, "delay to wait after the last modification to initate a sync")
	//forceRemote := flag.Bool("force-remote", false, "force fetching data blobs from object storage")
	//disableRemote := flag.Bool("disable-remote", false, "disable fetching data blobs from object storage")
	configFile := flag.String("config-file", filepath.Join(pathutil.ConfigDir(), "fs_client.yaml"), "confg file path")
	configProfile := flag.String("config-profile", "default", "config profile name")

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)
	ref := flag.Arg(1)

	profile, err := loadProfile(*configFile, *configProfile)
	if err != nil {
		fmt.Printf("failed to load config profile %s at %s: %v\n", *configProfile, *configFile, err)
		os.Exit(1)
	}

	if profile == nil {
		fmt.Printf("please setup a config file at %s\n", *configFile)
		os.Exit(1)
	}

	// Cache setup, follow XDG spec
	cacheDir := filepath.Join(pathutil.CacheDir(), "fs", fmt.Sprintf("%s_%s", mountpoint, ref))

	if _, err := os.Stat(cacheDir); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(cacheDir, 0700); err != nil {
				fmt.Printf("failed to create cache dir: %v\n", err)
				os.Exit(1)
			}
		}

	} else {
		if *resetCache {
			if err := os.RemoveAll(cacheDir); err != nil {
				fmt.Printf("failed to reset cache: %v\n", err)
				os.Exit(1)
			}
			if err := os.MkdirAll(cacheDir, 0700); err != nil {
				fmt.Printf("failed to re-create cache dir: %v\n", err)
				os.Exit(1)
			}
		}
	}

	// Setup the clients for BlobStash
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("failed to get hostname: %v\n", err)
		os.Exit(1)
	}

	clientUtil := clientutil.NewClientUtil(profile.Endpoint,
		clientutil.WithAPIKey(profile.APIKey),
		clientutil.WithHeader(ctxutil.FileTreeHostnameHeader, hostname),
		clientutil.WithHeader(ctxutil.NamespaceHeader, "rwfs-"+ref),
		clientutil.EnableMsgpack(),
		clientutil.EnableSnappyEncoding(),
	)

	bs := blobstore.New(clientUtil)
	kvs := kvstore.New(clientUtil)

	authOk, err := clientUtil.CheckAuth()
	if err != nil {
		fmt.Printf("failed to contact BlobStash: %v\n", err)
		os.Exit(1)
	}

	if !authOk {
		fmt.Printf("bad API key\n")
		os.Exit(1)
	}

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("helloworld"),
		fuse.Subtype("hellofs"),
		fuse.LocalVolume(),
		fuse.VolumeName("Hello world!"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, &FS{
		clientUtil: clientUtil,
		bs:         bs,
		kvs:        kvs,
		ref:        ref,
	})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// FS implements the hello world file system.
type FS struct {
	up         *writer.Uploader
	kvs        *kvstore.KvStore
	bs         *blobstore.BlobStore
	clientUtil *clientutil.ClientUtil
	ref        string
}

func (fs *FS) remotePath(path string) string {
	return fmt.Sprintf("/api/filetree/fs/fs/%s/%s", fs.ref, path)
}

// getNode fetches the node at path from BlobStash, like a "remote stat".
func (fs *FS) getNode(path string) (*Node, error) {
	return fs.getNodeAsOf(path, 0)
}

// getNode fetches the node at path from BlobStash, like a "remote stat".
func (fs *FS) getNodeAsOf(path string, asOf int64) (*Node, error) {
	resp, err := fs.clientUtil.Get(
		fs.remotePath(path),
		clientutil.WithQueryArg("as_of", strconv.FormatInt(asOf, 10)),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		if err.IsNotFound() {
			// Return nil as ENOENT
			return nil, nil
		}
		return nil, err
	}

	node := &Node{}
	if err := clientutil.Unmarshal(resp, node); err != nil {
		return nil, err
	}

	node.AsOf = asOf

	return node, nil
}

func (fs *FS) Root() (fs.Node, error) {
	fmt.Printf("FS=%+v\n", fs)
	root, err := fs.getNode("/")
	fmt.Printf("root=%v, %v\n", root, err)
	if err != nil {
		return nil, err
	}
	return Dir{"/", fs, root}, nil
}

type Dir struct {
	path string
	fs   *FS
	node *Node
}

func (d Dir) FTNode() (*Node, error) {
	if d.node != nil {
		return d.node, nil
	}
	n, err := d.fs.getNode(d.path)
	if err != nil {
		return nil, err
	}
	d.node = n
	return n, nil
}

func (d Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	n, err := d.FTNode()
	if err != nil {
		return err
	}
	if d.path == "/" {
		a.Inode = 1
	}
	a.Mode = os.ModeDir | os.FileMode(n.Mode())
	return nil
}

func (d Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	n, err := d.FTNode()
	if err != nil {
		return nil, err
	}
	for _, child := range n.Children {
		if child.Name == name {
			if child.IsFile() {
				return File{
					path: filepath.Join(d.path, name),
					fs:   d.fs,
					node: nil,
				}, nil
			} else {
				return Dir{
					path: filepath.Join(d.path, name),
					fs:   d.fs,
					node: nil,
				}, nil

			}
		}
	}
	return nil, fuse.ENOENT
}

func t(ty string) fuse.DirentType {
	switch ty {
	case "dir":
		return fuse.DT_Dir
	default:
		return fuse.DT_File
	}
}

func (d Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	n, err := d.FTNode()
	if err != nil {
		return nil, err
	}

	out := []fuse.Dirent{}
	for _, child := range n.Children {
		out = append(out, fuse.Dirent{Name: child.Name, Type: t(child.Type)})
	}
	return out, nil
}

// File implements both Node and Handle for the hello file.
type File struct {
	path string
	fs   *FS
	node *Node
}

func (f File) FTNode() (*Node, error) {
	if f.node != nil {
		return f.node, nil
	}
	n, err := f.fs.getNode(f.path)
	if err != nil {
		return nil, err
	}
	f.node = n
	return n, nil
}

type FileHandle struct {
	f *File
	r *filereader.File
}

func (f File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	fmt.Printf("Open\n")
	fh := &FileHandle{
		f: &f,
		r: nil,
	}
	resp.Flags |= fuse.OpenKeepCache
	return fh, nil
}

func (fh *FileHandle) Reader() (*filereader.File, error) {
	if fh.r == nil {
		n, err := fh.f.FTNode()
		if err != nil {
			return nil, err
		}
		blob, err := fh.f.fs.bs.Get(context.Background(), n.Ref)
		if err != nil {
			return nil, err
		}
		meta, err := rnode.NewNodeFromBlob(n.Ref, blob)
		if err != nil {
			return nil, fmt.Errorf("failed to build node from blob \"%s\": %v", blob, err)
		}

		fh.r = filereader.NewFile(context.Background(), fh.f.fs.bs, meta, nil)
	}
	return fh.r, nil
}

func (fh File) Attr(ctx context.Context, a *fuse.Attr) error {
	n, err := fh.FTNode()
	if err != nil {
		return err
	}

	// a.Inode = 2
	a.Mode = os.FileMode(n.Mode())
	a.Size = uint64(n.Size)
	return nil
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	fh.r.Close()
	fh.r = nil
	fmt.Printf("Release\n")
	return nil
}

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	r, err := fh.Reader()
	if err != nil {
		return err
	}
	buf := make([]byte, req.Size)
	if _, err := r.ReadAt(buf, req.Offset); err != nil {
		return err
	}
	resp.Data = buf
	return nil
}
