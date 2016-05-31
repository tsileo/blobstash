package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"

	log "github.com/inconshreveable/log15"
	"golang.org/x/net/context"

	"github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/hub"
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
	log        log.Logger
	applyFuncs map[string]func(string, []byte) error // map[<metadata type>]<load func>
	hub        *hub.Hub
}

func New(logger log.Logger, hub *hub.Hub) (*Meta, error) {
	meta := &Meta{
		log:        logger,
		hub:        hub,
		applyFuncs: map[string]func(string, []byte) error{},
	}
	meta.hub.Subscribe("meta", meta.newBlobCallback)
	return meta, nil
}

func (m *Meta) newBlobCallback(ctx context.Context, blob *blob.Blob) error {
	metaType, metaData, isMeta := IsMetaBlob(blob.Data)
	m.log.Debug("newBlobCallback", "is_meta", isMeta, "meta_type", metaType)
	if isMeta {
		// TODO(tsileo): ensure the type is registered
		return m.applyFuncs[metaType](blob.Hash, metaData)
	}
	return nil
}

func (m *Meta) RegisterApplyFunc(t string, f func(string, []byte) error) {
	m.applyFuncs[t] = f
}
func (m *Meta) Build(data MetaData) (*blob.Blob, error) {
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
		return nil, fmt.Errorf("failed to dump MetaData: %v", err)
	}
	binary.BigEndian.PutUint32(tmp[:], uint32(len(serialized)))
	buf.Write(tmp)
	buf.Write(serialized)
	m.log.Debug("meta blob", "data", buf.String())
	metaBlob := blob.New(buf.Bytes())
	return metaBlob, nil
}

// FIXME(ts): Scan

func IsMetaBlob(blob []byte) (string, []byte, bool) { // returns (string, bool) string => meta type
	// TODO add a test with a tiny blob
	if len(blob) < MetaBlobOverhead {
		return "", nil, false
	}
	if bytes.Equal(blob[0:MetaBlobOverhead], []byte(MetaBlobHeader)) {
		typeLen := int(binary.BigEndian.Uint32(blob[MetaBlobOverhead+4 : MetaBlobOverhead+8]))
		return string(blob[MetaBlobOverhead+8 : MetaBlobOverhead+8+typeLen]), blob[MetaBlobOverhead+12+typeLen : len(blob)], true
	}
	return "", nil, false
}
