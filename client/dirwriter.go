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
	//var cwr *WriteResult
	if len(dirdata) != 0 {
		cwrrc := make(chan *WriteResult, len(dirdata))
		for _, data := range dirdata {
			abspath := filepath.Join(path, data.Name())
			if data.IsDir() {
				wg.Add(1)
				go client.PutDirWg(abspath, wg, cwrrc)
				//if err != nil {
				//	log.Printf("Error putdir: %v", err)
				//}
				//cwrrc <- crw
			} else {

				if data.Mode() & os.ModeSymlink != 0 {
					log.Printf("Skipping SymLink %v\n", path)
					cwrrc <- &WriteResult{}		
				} else {
					wg.Add(1)
					//_, cwr, err = 
					go client.PutFileWg(abspath, wg, cwrrc)
				}
			}
			if err != nil {
				return
			}
		}
		wg.Wait()
		close(cwrrc)
		for ccwr := range cwrrc {
			if ccwr.Hash != "" {
				wr.Add(ccwr)
				hashes = append(hashes, ccwr.Hash)	
			}
		}
		//OuterLoop:
		//	for {
		//		var ccwr *WriteResult
		//		select {
		//		case ccwr = <-cwrrc:
		//			if ccwr != nil {
		//				wr.Add(ccwr)
		//				hashes = append(hashes, ccwr.Hash)
		//			}
		//		default:
		//			log.Println("breaking loop")
		//			break OuterLoop
		//		}
		//	}
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
	log.Printf("datadb: DirWriter(%v) WriteResult:%+v", path, wr)
	return
}

func (client *Client) PutDir(path string) (meta *Meta, wr *WriteResult, err error) {
	log.Printf("PutDir %v\n", path)
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
	log.Printf("PutDir %v done\n", path)
	return
}

func (client *Client) PutDirWg(path string, wg *sync.WaitGroup, cwrrc chan<- *WriteResult) {
	defer wg.Done()
	_, wr, err := client.PutDir(path)
	if err == nil {
		cwrrc <- wr	
	} else {
		log.Printf("Error putdir %v", err)
	}
	return
}
