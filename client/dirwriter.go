package client

import (
	"io/ioutil"
	"path/filepath"
	"crypto/sha1"
	"fmt"
	"os"
	"log"
	"sync"
	"sort"
	"github.com/garyburd/redigo/redis"
)

func (client *Client) DirWriter(path string) (wr *WriteResult, err error) {
	wg := &sync.WaitGroup{}
	con := client.Pool.Get()
	defer con.Close()
	wr = &WriteResult{Filename: filepath.Base(path)}
	dirdata, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
	h := sha1.New()
	hashes := []string{}
	dirdatalen := len(dirdata)
	if dirdatalen != 0 {
		cwrrc := make(chan *WriteResult, dirdatalen)
		errch := make(chan error, dirdatalen)
		for _, data := range dirdata {
			abspath := filepath.Join(path, data.Name())
			if data.IsDir() {
				wg.Add(1)
				go client.PutDirWg(abspath, wg, cwrrc, errch)
			} else {
				// Skip SymLink for now
				if data.Mode() & os.ModeSymlink == 0 {
					wg.Add(1)
					go client.PutFileWg(abspath, wg, cwrrc, errch)
				}
			}
		}
		wg.Wait()
		close(errch)
		close(cwrrc)
		for cerr := range errch {
			if cerr != nil {
				return  nil, cerr
			}
		}
		for ccwr := range cwrrc {
			if ccwr.Hash != "" {
				wr.Add(ccwr)
				hashes = append(hashes, ccwr.Hash)	
			}
		}
		// Sort the hashes by lexical order so the hash is deterministic
		sort.Strings(hashes)
		for _, hash := range hashes {
			h.Write([]byte(hash))
		}
		wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
		cnt, err := redis.Int(con.Do("SCARD", wr.Hash))
		if err != nil {
			return wr, err
		}
		if cnt == 0 {
			if len(hashes) > 0 {
				_, err = con.Do("SADD", redis.Args{}.Add(wr.Hash).AddFlat(hashes)...)
				if err != nil {
					return wr, err
				}	
			}
		} else {
			wr.AlreadyExists = true
		}
	} else {
		// If the dir is empty, hash the filename instead of members
		// so it doesn't break things.
		h.Write([]byte("emptydir:"))
		h.Write([]byte(wr.Filename))
		wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	//log.Printf("datadb: DirWriter(%v) WriteResult:%+v", path, wr)
	return
}

func (client *Client) PutDir(path string) (meta *Meta, wr *WriteResult, err error) {
	//log.Printf("PutDir %v\n", path)
	abspath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	wr, err = client.DirWriter(abspath)
	if err != nil {
		log.Printf("datadb: error DirWriter path:%v/abspath:%v/wr:%+v/err:%v\n", path, abspath, wr, err)
		return
	}
	meta = NewMeta()
	meta.Name = wr.Filename
	meta.Type = "dir"
	meta.Size = wr.Size
	meta.Hash = wr.Hash
	err = meta.Save(client.Pool)
	return
}

func (client *Client) PutDirWg(path string, wg *sync.WaitGroup, cwrrc chan<- *WriteResult, errch chan<- error) {
	defer wg.Done()
	_, wr, err := client.PutDir(path)
	if err != nil {
		log.Printf("Error PutDirWg %v", err)
		errch <- err
	} else {
		cwrrc <- wr	
	}
	return
}
