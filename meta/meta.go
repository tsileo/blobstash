package meta

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"sync"

	"github.com/dchest/blake2b"
	"github.com/tsileo/blobstash/router"
	"github.com/tsileo/blobstash/vkv"
)

var (
	MetaBlobHeader   = "#blobstash/meta\n"
	MetaBlobOverhead = len(MetaBlobHeader)
	NsBlobHeader     = "#blobstash/ns\n"
	NsBlobOverhead   = len(NsBlobHeader)
)

type MetaHandler struct {
	router *router.Router
	db     *vkv.DB
	stop   chan struct{}
}

func New(r *router.Router, db *vkv.DB) *MetaHandler {
	return &MetaHandler{
		router: r,
		stop:   make(chan struct{}),
		db:     db,
	}
}
func (mh *MetaHandler) Stop() {
	close(mh.stop)
}
func (mh *MetaHandler) processKvUpdate(wg sync.WaitGroup, blobs chan<- *router.Blob, kvUpdate <-chan *vkv.KeyValue) {
	wg.Add(1)
	defer wg.Done()
	for kv := range kvUpdate {
		log.Printf("metaHandler: kvupdate: %+v", kv)
		blob := CreateMetaBlob(kv)
		req := &router.Request{
			MetaBlob: true,
			Type:     router.Write,
		}
		hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
		if err := kv.SetMetaBlob(hash); err != nil {
			panic(err)
		}
		select {
		case blobs <- &router.Blob{Req: req, Hash: hash, Blob: blob}:
		case <-mh.stop:
			log.Printf("metaHandler: quitting")
			return
		}
	}
}

func (mh *MetaHandler) WatchKvUpdate(wg sync.WaitGroup, blobs chan<- *router.Blob, kvUpdate <-chan *vkv.KeyValue) error {
	for i := 0; i < 20; i++ {
		go mh.processKvUpdate(wg, blobs, kvUpdate)
	}
	return nil
}

func (mh *MetaHandler) Scan() error {
	blobs := make(chan string)
	errc := make(chan error, 1)
	req := &router.Request{
		MetaBlob: true,
		Type:     router.Read,
	}
	backend := mh.router.Route(req)
	go func() {
		errc <- backend.Enumerate(blobs)
	}()
	var i, j int
	for h := range blobs {
		i++
		applied, err := mh.db.MetaBlobApplied(h)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		// TODO a local cache for non meta blobs
		blob, err := backend.Get(h)
		if err != nil {
			return err
		}
		if IsMetaBlob(blob) {
			kv, err := DecodeMetaBlob(blob)
			if err != nil {
				return err
			}
			log.Printf("Scan: applying %+v", kv)
			rkv, err := mh.db.Put(kv.Key, kv.Value, kv.Version)
			if err != nil {
				return err
			}
			if err := rkv.SetMetaBlob(h); err != nil {
				return err
			}
			j++
		}
	}
	if err := <-errc; err != nil {
		return err
	}
	log.Printf("Scan: done, %d blobs scanned, %d blobs applied", i, j)
	return nil
}

func IsMetaBlob(blob []byte) bool {
	return bytes.Equal(blob[0:MetaBlobOverhead], []byte(MetaBlobHeader))
}

func IsNsBlob(blob []byte) bool {
	return bytes.Equal(blob[0:NsBlobOverhead], []byte(NsBlobHeader))
}

func CreateNsBlob(hexHash, namespace string) []byte {
	var buf bytes.Buffer
	buf.Write([]byte(NsBlobHeader))
	hash, err := hex.DecodeString(hexHash)
	if err != nil {
		panic(err)
	}
	buf.Write(hash)
	buf.WriteString(namespace)
	return buf.Bytes()
}
func DecodeNsBlob(blob []byte) (string, string) {
	hash := fmt.Sprintf("%x", blob[NsBlobOverhead:NsBlobOverhead+32])
	namespace := string(blob[NsBlobOverhead+32:])
	return hash, namespace
}

func CreateMetaBlob(kv *vkv.KeyValue) []byte {
	var buf bytes.Buffer
	buf.Write([]byte(MetaBlobHeader))
	buf.Write(encodeKv(kv))
	return buf.Bytes()
}

func DecodeMetaBlob(blob []byte) (*vkv.KeyValue, error) {
	return decodeKv(blob[MetaBlobOverhead:]), nil
}

func encodeKv(kv *vkv.KeyValue) []byte {
	data := make([]byte, len(kv.Key)+len(kv.Value)+16)
	binary.BigEndian.PutUint32(data[0:4], uint32(len(kv.Key)))
	copy(data[4:], []byte(kv.Key))
	binary.BigEndian.PutUint32(data[4+len(kv.Key):], uint32(len(kv.Value)))
	copy(data[8+len(kv.Key):], []byte(kv.Value))
	binary.BigEndian.PutUint64(data[cap(data)-8:], uint64(kv.Version))
	return data
}

func decodeKv(data []byte) *vkv.KeyValue {
	klen := int(binary.BigEndian.Uint32(data[0:4]))
	key := make([]byte, klen)
	copy(key[:], data[4:4+klen])
	vlen := int(binary.BigEndian.Uint32(data[4+klen : 8+klen]))
	value := make([]byte, vlen)
	copy(value[:], data[8+klen:8+klen+vlen])
	version := int(binary.BigEndian.Uint64(data[len(data)-8:]))
	return &vkv.KeyValue{
		Key:     string(key),
		Value:   string(value),
		Version: version,
	}
}
