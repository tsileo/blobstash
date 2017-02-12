/*

Package oplog provides an HTTP Server-Sent Events (SSE) endpoint for real-time replication of the BlobStore.

*/
package oplog // import "a4.io/blobstash/pkg/oplog"

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/hub"

	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
)

type Oplog struct {
	broker    *Broker
	hub       *hub.Hub
	log       log.Logger
	heartbeat *time.Ticker
}

type Op struct {
	Event, Data string
}

func New(logger log.Logger, conf *config.Config, h *hub.Hub) (*Oplog, error) {
	oplog := &Oplog{
		log:       logger,
		heartbeat: time.NewTicker(30 * time.Second),
		broker: &Broker{
			log:            logger.New("submodule", "broker"),
			clients:        make(map[chan *Op]bool),
			newClients:     make(chan (chan *Op)),
			defunctClients: make(chan (chan *Op)),
			ops:            make(chan *Op),
		},
		hub: h,
	}
	oplog.init()
	return oplog, nil
}

func (o *Oplog) newBlobCallback(ctx context.Context, blob *blob.Blob, _ interface{}) error {
	// Send the blob hash to the broker
	o.broker.ops <- &Op{Event: "blob", Data: blob.Hash}
	return nil
}

func (o *Oplog) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	// Register the SSE HTTP endpoint
	r.Handle("/", basicAuth(o.broker))
}

func (o *Oplog) init() {
	// Start the SSE broker worker
	o.broker.start()
	// Register to the new blob event
	o.hub.Subscribe(hub.NewBlob, "oplog", o.newBlobCallback)

	go func() {
		for {
			<-o.heartbeat.C
			o.broker.ops <- &Op{Event: "heartbeat", Data: ""}
		}
	}()
}

type Broker struct {
	log            log.Logger
	clients        map[chan *Op]bool
	newClients     chan chan *Op
	defunctClients chan chan *Op
	ops            chan *Op
}

func (b *Broker) start() {
	go func() {
		for {
			select {
			case s := <-b.newClients:
				// There is a new client attached and we
				// want to start sending them messages.
				b.clients[s] = true
				b.log.Debug("added new client")

			case s := <-b.defunctClients:
				// A client has dettached and we want to
				// stop sending them messages.
				delete(b.clients, s)
				close(s)
				b.log.Debug("removed client")

			case op := <-b.ops:
				// There is a new message to send.  For each
				// attached client, push the new message
				// into the client's message channel.
				for s, _ := range b.clients {
					s <- op
				}
				b.log.Info("message sent", "op", op, "clients_count", len(b.clients))
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
	opChan := make(chan *Op)

	// Add this client to the map of those that should
	// receive updates
	b.newClients <- opChan

	// Listen to the closing of the http connection via the CloseNotifier
	notify := w.(http.CloseNotifier).CloseNotify()
	go func() {
		<-notify
		// Remove this client from the map of attached clients
		// when `EventHandler` exits.
		b.defunctClients <- opChan
	}()

	// Set the headers related to event streaming.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Send an initial heartbeat
	fmt.Fprintf(w, "event: heartbeat\ndata: \n\n")
	f.Flush()

	for {

		// Read from our messageChan.
		op, open := <-opChan

		if !open {
			// If our messageChan was closed, this means that the client has
			// disconnected.
			break
		}

		// Write to the ResponseWriter, `w`.
		fmt.Fprintf(w, "event: %s\n", op.Event)
		fmt.Fprintf(w, "data: %s\n\n", op.Data)

		// Flush the response.  This is only possible if
		// the repsonse supports streaming.
		f.Flush()
	}
}
