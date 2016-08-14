package hub

import (
	"golang.org/x/net/context"

	log "github.com/inconshreveable/log15"

	"github.com/tsileo/blobstash/pkg/blob"
	_ "github.com/tsileo/blobstash/pkg/ctxutil"
)

type EventType int

const (
	NewBlob EventType = iota
	ScanBlob
	GarbageCollection
)

type Hub struct {
	log         log.Logger
	subscribers map[EventType]map[string]func(context.Context, *blob.Blob) error
}

func (h *Hub) Subscribe(etype EventType, name string, callback func(context.Context, *blob.Blob) error) {
	h.log.Info("new subscription", "type", etype, "name", name)
	h.subscribers[etype][name] = callback
}

func (h *Hub) newEvent(ctx context.Context, etype EventType, blob *blob.Blob) error {
	l := h.log.New("type", etype, "blob", blob)
	l.Debug("new event")
	for name, callback := range h.subscribers[etype] {
		h.log.Debug("triggering callback", "name", name)
		if err := callback(ctx, blob); err != nil {
			return err
		}
	}
	return nil
}

func (h *Hub) NewBlobEvent(ctx context.Context, blob *blob.Blob) error {
	return h.newEvent(ctx, NewBlob, blob)
}

func (h *Hub) ScanBlobEvent(ctx context.Context, blob *blob.Blob) error {
	return h.newEvent(ctx, ScanBlob, blob)
}

func New(logger log.Logger) *Hub {
	logger.Debug("init")
	return &Hub{
		log: logger,
		subscribers: map[EventType]map[string]func(context.Context, *blob.Blob) error{
			NewBlob:  map[string]func(context.Context, *blob.Blob) error{},
			ScanBlob: map[string]func(context.Context, *blob.Blob) error{},
		},
	}
}
