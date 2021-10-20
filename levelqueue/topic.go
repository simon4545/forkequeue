package levelqueue

import (
	"forkequeue/internal/util"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"sync"
	"sync/atomic"
)

type Topic struct {
	sync.RWMutex

	name      string
	queue     *levelQueue
	startChan chan int
	exitChan  chan int
	exitFlag  int32

	waitGroup util.WaitGroupWrapper

	idFactory *guidFactory

	server *Server
}

func NewTopic(topicName string, server *Server) *Topic {
	t := &Topic{
		name:      topicName,
		startChan: make(chan int, 1),
		exitChan:  make(chan int),
		idFactory: NewGUIDFactory(1),
		server:    server,
	}
	t.queue = NewQueue(topicName, "")

	return t
}

func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m)
	if err != nil {
		return err
	}
	return nil
}

func (t *Topic) put(m *Message) error {
	select {
	default:
		err := writeMessageToQueue(m, t.queue)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Topic) message(m *Message) error {
	select {
	default:
		err := writeMessageToQueue(m, t.queue)
		if err != nil {
			return err
		}
	}
	return nil
}
