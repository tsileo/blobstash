package pubsub

import (
	"github.com/bitly/go-notify"
)

type PubSub struct {
	Channel string
	Msgc chan interface{}
}

func NewPubSub(channel string) *PubSub {
	return &PubSub{Channel: channel, Msgc: make(chan interface{})}
}

func (ps *PubSub) Publish(msg interface{}) error {
	return notify.Post(ps.Channel, msg)
}

func (ps *PubSub) Listen() {
	notify.Start(ps.Channel, ps.Msgc)
}

func (ps *PubSub) Close() error {
	return notify.Stop(ps.Channel, ps.Msgc)
}
