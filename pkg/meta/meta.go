package meta

import (
	"bytes"
	"encoding/binary"

	log "github.com/inconshreveable/log15"

	"github.com/tsileo/blobstash/pkg/blobstore"
)

var (
	MetaBlobHeader   = "#blobstash/meta\n"
	MetaBlobOverhead = len(MetaBlobHeader)
)

type MetaData interface {
	Type() string
	Dump() []byte
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
	// <meta blob header> + <type size> + <type bytes> + <data size> + <data>
	buf.Write([]byte(MetaBlobHeader))
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp[:], uint32(len(data.Type())))
	buf.Write(tmp)
	buf.WriteString(data.Type())
	serialized := data.Dump()
	binary.BigEndian.PutUint32(tmp[:], uint32(len(serialized)))
	buf.Write(tmp)
	buf.Write(serialized)
	m.log.Info("meta blob", "data", buf.String())
	return nil
}

// FIXME(ts): Scan

func IsMetaBlob(blob []byte) bool {
	// TODO add a test with a tiny blob
	if len(blob) < MetaBlobOverhead {
		return false
	}
	return bytes.Equal(blob[0:MetaBlobOverhead], []byte(MetaBlobHeader))
}
