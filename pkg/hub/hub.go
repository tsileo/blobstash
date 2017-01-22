package hub // import "a4.io/blobstash/pkg/hub"

import (
	"golang.org/x/net/context"

	log "github.com/inconshreveable/log15"

	"a4.io/blobstash/pkg/blob"
	_ "a4.io/blobstash/pkg/ctxutil"
)

type EventType int

const (
	NewBlob EventType = iota
	ScanBlob
	GarbageCollection
	AppUpdate
)

type AppUpdateData struct {
	Ref          string
	Name         string
	RawAppConfig []byte
}

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

func (h *Hub) NewAppUpdateEvent(ctx context.Context, blob *blob.Blob, data interface{}) error {
	return h.newEvent(ctx, AppUpdate, blob, data)
}

func (h *Hub) NewBlobEvent(ctx context.Context, blob *blob.Blob, data interface{}) error {
	return h.newEvent(ctx, NewBlob, blob, data)
}

func (h *Hub) ScanBlobEvent(ctx context.Context, blob *blob.Blob, data interface{}) error {
	return h.newEvent(ctx, ScanBlob, blob, data)
}

func New(logger log.Logger) *Hub {
	logger.Debug("init")
	return &Hub{
		log: logger,
		subscribers: map[EventType]map[string]func(context.Context, *blob.Blob, interface{}) error{
			NewBlob:   map[string]func(context.Context, *blob.Blob, interface{}) error{},
			ScanBlob:  map[string]func(context.Context, *blob.Blob, interface{}) error{},
			AppUpdate: map[string]func(context.Context, *blob.Blob, interface{}) error{},
		},
	}
}
