package meta // import "a4.io/blobstash/pkg/meta"

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	log "github.com/inconshreveable/log15"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/hub"
)

var (
	metaBlobHeader   = "#blobstash/meta\n"
	metaBlobVersion  = 1
	metaBlobOverhead = len(metaBlobHeader)
)

// MetaData is the interface that must be implemented by the different meta data types
type MetaData interface {
	Type() string
	Dump() ([]byte, error)
	// Load([]byte)
}

// Meta holds the meta manager
type Meta struct {
	log        log.Logger
	applyFuncs map[string]func(string, []byte) error // map[<metadata type>]<load func>
	hub        *hub.Hub
}

// New initializes a meta manager
func New(logger log.Logger, chub *hub.Hub) (*Meta, error) {
	meta := &Meta{
		log:        logger,
		hub:        chub,
		applyFuncs: map[string]func(string, []byte) error{},
	}
	// Subscribe to "new blob" notification
	meta.hub.Subscribe(hub.NewBlob, "meta", meta.newBlobCallback)
	meta.hub.Subscribe(hub.ScanBlob, "meta", meta.newBlobCallback)
	// XXX(tsileo): register to ScanBlob event too?
	return meta, nil
}

func (m *Meta) newBlobCallback(ctx context.Context, blob *blob.Blob, _ interface{}) error {
	metaType, metaData, isMeta := IsMetaBlob(blob.Data)
	m.log.Debug("newBlobCallback", "is_meta", isMeta, "meta_type", metaType, "blob_size", len(blob.Data))
	if isMeta {
		m.log.Debug("blob callback", "blob", string(blob.Data))
		// TODO(tsileo): should we check for already applied blobs and use the same callback for both scan and new blob?
		if _, ok := m.applyFuncs[metaType]; !ok {
			return fmt.Errorf("Unknown meta type \"%s\"", metaType)
		}
		return m.applyFuncs[metaType](blob.Hash, metaData)
	}
	return nil
}

// RegisterApplyFunc registers a callback func for the given meta type
func (m *Meta) RegisterApplyFunc(t string, f func(string, []byte) error) {
	m.applyFuncs[t] = f
}

// Build convert the MetaData into a blo
func (m *Meta) Build(data MetaData) (*blob.Blob, error) {
	var buf bytes.Buffer
	// <meta blob header> + <meta blob version> + <type size> + <type bytes> + <data size> + <data>
	buf.Write([]byte(metaBlobHeader))
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp[:], uint32(metaBlobVersion))
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

// Scan does nothing for the moment
func (m *Meta) Scan() error {
	// FIXME(ts): Scan
	return nil
}

// IsMetaBlob returns true if the blob is "mata blob" (an encoded internal piece of data.
// It returns the meta type as a string, and the blob if the blob is an actual meta blob.
func IsMetaBlob(blob []byte) (string, []byte, bool) { // returns (string, bool) string => meta type
	// TODO add a test with a tiny blob
	if len(blob) < metaBlobOverhead {
		return "", nil, false
	}
	if bytes.Equal(blob[0:metaBlobOverhead], []byte(metaBlobHeader)) {
		typeLen := int(binary.BigEndian.Uint32(blob[metaBlobOverhead+4 : metaBlobOverhead+8]))
		return string(blob[metaBlobOverhead+8 : metaBlobOverhead+8+typeLen]), blob[metaBlobOverhead+12+typeLen : len(blob)], true
	}
	return "", nil, false
}
