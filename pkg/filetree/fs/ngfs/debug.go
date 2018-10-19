package main

import (
	"os"
	"strconv"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	ps "github.com/mitchellh/go-ps"
	"golang.org/x/net/context"
)

var mu sync.Mutex
var pidCache = map[uint32]string{}

type fdDebug struct {
	FSType   string `json:"fs_type"`
	Path     string `json:"path"`
	PName    string `json:"pname"`
	PID      uint32 `json:"pid"`
	RW       bool   `json:"rw"`
	openedAt time.Time
	OpenedAt string `json:"opened_at"`
}

type dataNode struct {
	data []byte
	f    func() ([]byte, error)
}

func (d *dataNode) Attr(ctx context.Context, a *fuse.Attr) error {
	if d.f != nil {
		var err error
		d.data, err = d.f()
		if err != nil {
			return err
		}
	}
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	a.Mode = 0644
	a.Size = uint64(len(d.data))
	return nil
}

func (d *dataNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if d.f != nil {
		var err error
		d.data, err = d.f()
		if err != nil {
			return nil, err
		}
	}

	resp.Flags |= fuse.OpenDirectIO
	return fs.DataHandle(d.data), nil
}

type counterNode struct {
	val int
}

func (c *counterNode) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	a.Mode = 0644
	a.Size = uint64(len(strconv.Itoa(c.val)))
	return nil
}

func (c *counterNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	resp.Flags |= fuse.OpenDirectIO
	return fs.DataHandle([]byte(strconv.Itoa(c.val))), nil
}

type counters struct {
	Tree  *fs.Tree
	index map[string]*counterNode
	mu    sync.Mutex
}

func newCounters() *counters {
	return &counters{index: map[string]*counterNode{}, Tree: &fs.Tree{}}
}

func (c *counters) register(name string) {
	c.index[name] = &counterNode{}
	c.Tree.Add(name, c.index[name])
}

func (c *counters) incr(name string) {
	c.mu.Lock()
	c.index[name].val++
	c.mu.Unlock()
}

// Fetch the process name for the given PID
func getProcName(pid uint32) string {
	mu.Lock()
	defer mu.Unlock()

	if pname, ok := pidCache[pid]; ok {
		return pname
	}

	p, err := ps.FindProcess(int(pid))
	if err != nil {
		panic(err)
	}
	if p == nil {
		return "<unk>"
	}

	exec := p.Executable()
	pidCache[pid] = exec

	return exec
}
