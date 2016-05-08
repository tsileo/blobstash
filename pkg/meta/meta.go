package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"

	log "github.com/inconshreveable/log15"
	"golang.org/x/net/context"

	"github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/blobstore"
)

var (
	MetaBlobHeader   = "#blobstash/meta\n"
	MetaBlobVersion  = 1
	MetaBlobOverhead = len(MetaBlobHeader)
)

type MetaData interface {
	Type() string
	Dump() ([]byte, error)
	// Load([]byte)
}

type Meta struct {
	blobStore  *blobstore.BlobStore
	log        log.Logger
	applyFuncs map[string]func([]byte) error // map[<metadata type>]<load func>
}

func New(logger log.Logger, blobStore *blobstore.BlobStore) (*Meta, error) {
	return &Meta{
		log:        logger,
		blobStore:  blobStore,
		applyFuncs: map[string]func([]byte) error{},
	}, nil
}

func (m *Meta) RegisterApplyFunc(t string, f func([]byte) error) {
	m.applyFuncs[t] = f
}

func (m *Meta) Save(data MetaData) error {
	var buf bytes.Buffer
	// <meta blob header> + <meta blob version> + <type size> + <type bytes> + <data size> + <data>
	buf.Write([]byte(MetaBlobHeader))
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp[:], uint32(MetaBlobVersion))
	buf.Write(tmp)
	binary.BigEndian.PutUint32(tmp[:], uint32(len(data.Type())))
	buf.Write(tmp)
	buf.WriteString(data.Type())
	serialized, err := data.Dump()
	if err != nil {
		return fmt.Errorf("failed to dump MetaData: %v", err)
	}
	binary.BigEndian.PutUint32(tmp[:], uint32(len(serialized)))
	buf.Write(tmp)
	buf.Write(serialized)
	m.log.Debug("meta blob", "data", buf.String())
	return m.blobStore.Put(context.Background(), blob.New(buf.Bytes()))
}

// FIXME(ts): Scan

func IsMetaBlob(blob []byte) bool {
	// TODO add a test with a tiny blob
	if len(blob) < MetaBlobOverhead {
		return false
	}
	return bytes.Equal(blob[0:MetaBlobOverhead], []byte(MetaBlobHeader))
}
