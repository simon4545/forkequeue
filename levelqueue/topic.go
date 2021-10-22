package levelqueue

import (
	"forkequeue/internal/util"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Topic struct {
	sync.RWMutex

	name      string
	queue     *levelQueue
	startChan chan int
	exitChan  chan int
	exitFlag  int32

	readChan chan []byte

	waitGroup util.WaitGroupWrapper

	idFactory *guidFactory

	server *Server
}

func NewTopic(topicName string, server *Server) *Topic {
	t := &Topic{
		name:      topicName,
		startChan: make(chan int, 1),
		exitChan:  make(chan int),
		readChan:  make(chan []byte),
		idFactory: NewGUIDFactory(1),
		server:    server,
	}
	t.queue = NewQueue(topicName, server.getOpts().DataPath)

	t.server.Notify(t)
	return t
}

func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
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

func (t *Topic) Close() error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	log.Printf("TOPIC(%s): closing\n", t.name)

	close(t.exitChan)

	return t.queue.Close()
}

func (t *Topic) ReadChan() <-chan []byte {
	return t.queue.Pop()
}

func (t *Topic) GenerateID() MessageID {
	var i int64 = 0
	for {
		id, err := t.idFactory.NewGUID()
		if err == nil {
			return id.Hex()
		}

		time.Sleep(time.Millisecond)
		i++
	}
}
