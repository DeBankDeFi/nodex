package broker

import (
	"sync"
)

const MaxChannelSize = 512

type Broker[T any] struct {
	sync.Mutex
	subs map[chan *T]struct{}
}

func NewBroker[T any]() *Broker[T] {
	return &Broker[T]{
		subs: make(map[chan *T]struct{}),
	}
}

func (b *Broker[T]) Subscribe() chan *T {
	b.Lock()
	defer b.Unlock()
	msgCh := make(chan *T, MaxChannelSize)
	b.subs[msgCh] = struct{}{}
	return msgCh
}

func (b *Broker[T]) Unsubscribe(msgCh chan *T) {
	b.Lock()
	defer b.Unlock()
	if _, ok := b.subs[msgCh]; ok {
		delete(b.subs, msgCh)
		close(msgCh)
	}
}

func (b *Broker[T]) Publish(msg *T) {
	b.Lock()
	defer b.Unlock()
	for msgCh := range b.subs {
		select {
		case msgCh <- msg:
		default:
			if _, ok := b.subs[msgCh]; ok {
				delete(b.subs, msgCh)
				close(msgCh)
			}
		}
	}
}
