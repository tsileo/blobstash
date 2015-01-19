package meta

import (
	"bytes"
	"encoding/binary"
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
)

type MetaHandler struct {
	router *router.Router
}

func New(r *router.Router) *MetaHandler {
	return &MetaHandler{router: r}
}

func (mh *MetaHandler) WatchKvUpdate(wg sync.WaitGroup, kvUpdate <-chan *vkv.KeyValue) error {
	for kv := range kvUpdate {
		go func(kv *vkv.KeyValue) {
			wg.Add(1)
			defer wg.Done()
			log.Printf("MetaHandler get kvupdate: %+v", kv)
			req := &router.Request{
				MetaBlob: true,
				Type:     router.Write,
			}
			backend := mh.router.Route(req)
			blob := CreateMetaBlob(kv)
			hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
			if backend.Exists(hash) {
				return
			}
			if err := backend.Put(hash, blob); err != nil {
				panic(err)
			}
		}(kv)
	}
	return nil
}

func (mh *MetaHandler) Scan() error {
	// TODO scan for meta blobs with a local cache.
	return nil
}

func IsMetaBlob(blob []byte) bool {
	return bytes.Equal(blob[0:MetaBlobOverhead], []byte(MetaBlobHeader))
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
