package oplog // import "a4.io/blobstash/pkg/oplog"

import (
	"context"
	"fmt"
	"net/http"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/hub"

	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
)

type Oplog struct {
	broker *Broker
	hub    *hub.Hub
	log    log.Logger
}

func New(logger log.Logger, conf *config.Config, h *hub.Hub) (*Oplog, error) {
	oplog := &Oplog{
		log: logger,
		broker: &Broker{
			log:            logger.New("submodule", "broker"),
			clients:        make(map[chan string]bool),
			newClients:     make(chan (chan string)),
			defunctClients: make(chan (chan string)),
			messages:       make(chan string),
		},
		hub: h,
	}
	return oplog, nil
}

func (o *Oplog) newBlobCallback(ctx context.Context, blob *blob.Blob, _ interface{}) error {
	o.broker.messages <- blob.Hash
	return nil
}

func (o *Oplog) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/", basicAuth(o.broker))
}

func (o *Oplog) init() {
	o.broker.start()
	o.hub.Subscribe(hub.NewBlob, "oplog", o.newBlobCallback)
}

type Broker struct {
	log            log.Logger
	clients        map[chan string]bool
	newClients     chan chan string
	defunctClients chan chan string
	messages       chan string
}

func (b *Broker) start() {
	go func() {
		for {
			select {

			case s := <-b.newClients:
				// There is a new client attached and we
				// want to start sending them messages.
				b.clients[s] = true
				b.log.Debug("added new client", "client", s)

			case s := <-b.defunctClients:
				// A client has dettached and we want to
				// stop sending them messages.
				delete(b.clients, s)
				close(s)
				b.log.Debug("removed client", "client", s)

			case msg := <-b.messages:
				// There is a new message to send.  For each
				// attached client, push the new message
				// into the client's message channel.
				for s, _ := range b.clients {
					s <- msg
				}
				b.log.Info("message sent", "msg", msg, "clients_count", len(b.clients))
			}
		}
	}()
}

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	// Create a new channel, over which the broker can
	// send this client messages.
	messageChan := make(chan string)

	// Add this client to the map of those that should
	// receive updates
	b.newClients <- messageChan

	// Listen to the closing of the http connection via the CloseNotifier
	notify := w.(http.CloseNotifier).CloseNotify()
	go func() {
		<-notify
		// Remove this client from the map of attached clients
		// when `EventHandler` exits.
		b.defunctClients <- messageChan
	}()

	// Set the headers related to event streaming.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for {

		// Read from our messageChan.
		msg, open := <-messageChan

		if !open {
			// If our messageChan was closed, this means that the client has
			// disconnected.
			break
		}

		// Write to the ResponseWriter, `w`.
		fmt.Fprintf(w, "data: Message: %s\n\n", msg)

		// Flush the response.  This is only possible if
		// the repsonse supports streaming.
		f.Flush()
	}
}
