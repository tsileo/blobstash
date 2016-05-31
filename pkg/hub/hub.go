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
)

type Hub struct {
	log         log.Logger
	subscribers map[string]func(context.Context, *blob.Blob) error
}

func (h *Hub) Subscribe(name string, callback func(context.Context, *blob.Blob) error) {
	h.log.Info("new subscription", "name", name)
	h.subscribers[name] = callback
}

func (h *Hub) NewBlobEvent(ctx context.Context, blob *blob.Blob) error {
	h.log.Debug("NewBLobEvent")
	for name, callback := range h.subscribers {
		h.log.Debug("triggering callback", "name", name)
		if err := callback(ctx, blob); err != nil {
			return err
		}
	}
	return nil
}

func New(logger log.Logger) *Hub {
	logger.Debug("init")
	return &Hub{
		log:         logger,
		subscribers: map[string]func(context.Context, *blob.Blob) error{},
	}
}
