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

// DirWriter reads the directory and upload it.
func (client *Client) DirWriter(txID, path string) (wr *WriteResult, err error) {
	wg := &sync.WaitGroup{}
	con := client.Pool.Get()
	defer con.Close()
	if _, err := con.Do("TXINIT", txID); err != nil {
		return wr, err
	}
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
				go func(path string) {
					defer wg.Done()
					_, wr, err := client.PutDir(txID, path)
					if err != nil {
						errch <- err
					} else {
						cwrrc <- wr	
					}
				}(abspath)
			} else {
				// Skip SymLink for now
				if data.Mode() & os.ModeSymlink == 0 {
					wg.Add(1)
					go func(path string) {
						defer wg.Done()
						_, wr, err := client.PutFile(txID, path)
						if err != nil {
							errch <- err
						} else {
							cwrrc <- wr	
						}
					}(abspath)
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
		// Check if the directory meta already exists 
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
		// If the directory is empty, hash the filename instead of members
		// so an empty directory won't invalidate the directory top hash.
		h.Write([]byte("emptydir:"))
		h.Write([]byte(wr.Filename))
		wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	//log.Printf("datadb: DirWriter(%v) WriteResult:%+v", path, wr)
	return
}

// PutDir upload a directory, it returns the saved Meta,
// a WriteResult containing infos about uploaded blobs.
func (client *Client) PutDir(txID, path string) (meta *Meta, wr *WriteResult, err error) {
	//log.Printf("PutDir %v\n", path)
	abspath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	wr, err = client.DirWriter(txID, abspath)
	if err != nil {
		log.Printf("datadb: error DirWriter path:%v/abspath:%v/wr:%+v/err:%v\n", path, abspath, wr, err)
		return
	}
	meta = NewMeta()
	meta.Name = wr.Filename
	meta.Type = "dir"
	meta.Size = wr.Size
	meta.Hash = wr.Hash
	err = meta.Save(txID, client.Pool)
	return
}

