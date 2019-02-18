package hub // import "a4.io/blobstash/pkg/hub"

import (
	"context"

	log "github.com/inconshreveable/log15"

	"a4.io/blobstash/pkg/blob"
	_ "a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/filetree/filetreeutil/node"
)

type EventType int

const (
	NewBlob EventType = iota
	ScanBlob
	GarbageCollection
	NewFiletreeNode
	FiletreeFSUpdate // TODO(tsileo): remove these events
	SyncRemoteBlob
	DeleteRemoteBlob
)

type Hub struct {
	log         log.Logger
	subscribers map[EventType]map[string]func(context.Context, *blob.Blob, interface{}) error
}

func (h *Hub) Subscribe(etype EventType, name string, callback func(context.Context, *blob.Blob, interface{}) error) {
	h.log.Info("new subscription", "type", etype, "name", name)
	h.subscribers[etype][name] = callback
}

func (h *Hub) newEvent(ctx context.Context, etype EventType, blob *blob.Blob, data interface{}) error {
	l := h.log.New("type", etype, "blob", blob, "data", data)
	l.Debug("new event")
	for name, callback := range h.subscribers[etype] {
		h.log.Debug("triggering callback", "name", name)
		if err := callback(ctx, blob, data); err != nil {
			return err
		}
	}
	return nil
}

func (h *Hub) NewBlobEvent(ctx context.Context, blob *blob.Blob, data interface{}) error {
	if err := h.newEvent(ctx, NewBlob, blob, data); err != nil {
		return err
	}
	_, isNodeBlob := node.IsNodeBlob(blob.Data)
	if isNodeBlob {
		n, err := node.NewNodeFromBlob(blob.Hash, blob.Data)
		if err != nil {
			return err
		}
		if err := h.newEvent(ctx, NewFiletreeNode, blob, n); err != nil {
			return nil
		}
	}
	return nil
}

func (h *Hub) ScanBlobEvent(ctx context.Context, blob *blob.Blob, data interface{}) error {
	return h.newEvent(ctx, ScanBlob, blob, data)
}

func (h *Hub) FiletreeFSUpdateEvent(ctx context.Context, blob *blob.Blob, data interface{}) error {
	return h.newEvent(ctx, FiletreeFSUpdate, blob, data)
}

func (h *Hub) NewDeleteRemoteBlobEvent(ctx context.Context, blob *blob.Blob, data interface{}) error {
	return h.newEvent(ctx, DeleteRemoteBlob, blob, data)
}

func (h *Hub) NewSyncRemoteBlobEvent(ctx context.Context, blob *blob.Blob, data interface{}) error {
	return h.newEvent(ctx, SyncRemoteBlob, blob, data)
}

func New(logger log.Logger) *Hub {
	logger.Debug("init")
	return &Hub{
		log: logger,
		subscribers: map[EventType]map[string]func(context.Context, *blob.Blob, interface{}) error{
			NewBlob:          map[string]func(context.Context, *blob.Blob, interface{}) error{},
			ScanBlob:         map[string]func(context.Context, *blob.Blob, interface{}) error{},
			FiletreeFSUpdate: map[string]func(context.Context, *blob.Blob, interface{}) error{},
			SyncRemoteBlob:   map[string]func(context.Context, *blob.Blob, interface{}) error{},
			NewFiletreeNode:  map[string]func(context.Context, *blob.Blob, interface{}) error{},
			DeleteRemoteBlob: map[string]func(context.Context, *blob.Blob, interface{}) error{},
		},
	}
}
