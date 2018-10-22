package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"text/tabwriter"
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

// build the magic .stats dir
func statsTree(cfs *FS) *fs.Tree {
	// Setup the counters
	cfs.counters.register("open")
	cfs.counters.register("open-ro")
	cfs.counters.register("open-ro-error")
	cfs.counters.register("open-rw")
	cfs.counters.register("open-rw-error")

	// Debug VFS mounted a /.stats
	statsTree := &fs.Tree{}
	statsTree.Add("started_at", &dataNode{data: []byte(startedAt.Format(time.RFC3339))})
	statsTree.Add("last_revision", &dataNode{f: func() ([]byte, error) {
		return []byte(strconv.FormatInt(cfs.lastRevision, 10)), nil
	}})
	statsTree.Add("fds.json", &dataNode{f: func() ([]byte, error) {
		for _, d := range cfs.openedFds {
			if d.OpenedAt == "" {
				d.OpenedAt = d.openedAt.Format(time.RFC3339)
			}
		}
		return json.Marshal(cfs.openedFds)
	}})
	statsTree.Add("fds", &dataNode{f: func() ([]byte, error) {
		var buf bytes.Buffer
		w := tabwriter.NewWriter(&buf, 0, 0, 1, ' ', tabwriter.TabIndent)
		for _, d := range cfs.openedFds {
			fmt.Fprintln(w, fmt.Sprintf("%s\t%d\t%s\t%v\t%s", d.Path, d.PID, d.PName, d.RW, d.openedAt.Format(time.RFC3339)))
		}
		w.Flush()
		return buf.Bytes(), nil
	}})
	statsTree.Add("open_logs", &dataNode{f: func() ([]byte, error) {
		var buf bytes.Buffer
		w := tabwriter.NewWriter(&buf, 0, 0, 1, ' ', tabwriter.TabIndent)
		for _, d := range cfs.openLogs {
			fmt.Fprintln(w, fmt.Sprintf("%s\t%d\t%s\t%v\t%s", d.Path, d.PID, d.PName, d.RW, d.openedAt.Format(time.RFC3339)))
		}
		w.Flush()
		return buf.Bytes(), nil
	}})
	statsTree.Add("open_logs.json", &dataNode{f: func() ([]byte, error) {
		for _, d := range cfs.openLogs {
			if d.OpenedAt == "" {
				d.OpenedAt = d.openedAt.Format(time.RFC3339)
			}
		}
		return json.Marshal(cfs.openLogs)
	}})
	statsTree.Add("counters", cfs.counters.Tree)
	return statsTree
}
