package levelqueue

import (
	"forkequeue/internal/util"
	"github.com/syndtr/goleveldb/leveldb/errors"
	util2 "github.com/syndtr/goleveldb/leveldb/util"
	"log"
	"math"
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

	inAckMessages map[MessageID]*Message
	inAckQ        inAckQueue
	inAckMutex    sync.Mutex
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

	t.initAckQueue()

	t.waitGroup.Wrap(t.pendingMsgLoop)

	t.server.Notify(t)
	return t
}

//init ack queue ;reload pending msg from local db
func (t *Topic) initAckQueue() {
	qSize := int(math.Max(1, float64(t.server.getOpts().MemQueueSize)/10))

	t.inAckMessages = make(map[MessageID]*Message)
	t.inAckQ = newInAckQueue(qSize)

	iter := t.server.pendingDB.NewIterator(util2.BytesPrefix([]byte(t.name)), nil)
	for iter.Next() {
		now := time.Now().UnixNano()
		key := iter.Key()
		value := iter.Value()
		msg, err := decodeAckMsg(value)
		if err != nil {
			continue
		}
		if msg.pri <= now {
			//expire msg put back in topic queue
			err = t.put(msg)
			if err != nil {
				continue
			}
			t.removeMsgInAckDB(key)
		} else {
			//add in memory Ack queue
			err := t.pushInAckMsg(msg)
			if err != nil {
				continue
			}
			t.addToInAckQueue(msg)
		}
	}
}

func (t *Topic) pendingMsgLoop() {
	workTicker := time.NewTicker(t.server.getOpts().QueueScanInterval)

	select {
	case <-t.exitChan:
		goto exit
	case <-t.startChan:
	}

	for {
		select {
		case <-workTicker.C:
			now := time.Now().UnixNano()
			t.processInAckQueue(now)
		case <-t.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("TOPIC(%s): closing ... pendingMsgLoop", t.name)
	workTicker.Stop()
}

func (t *Topic) FinishMessage(id MessageID) error {
	msg, err := t.popInAckMsg(id)
	if err != nil {
		return err
	}
	t.removeFromInAckQueue(msg)

	key := append([]byte(t.name), msg.ID[:]...)
	return t.removeMsgInAckDB(key)
}

func (t *Topic) StartInAckTimeOut(msg *Message, timeout time.Duration) error {
	now := time.Now()

	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()

	err := t.addToInAckDB(msg)
	if err != nil {
		return err
	}

	err = t.pushInAckMsg(msg)
	if err != nil {
		return err
	}
	t.addToInAckQueue(msg)
	return nil
}

func (t *Topic) pushInAckMsg(msg *Message) error {
	t.inAckMutex.Lock()
	_, ok := t.inAckMessages[msg.ID]
	if ok {
		t.inAckMutex.Unlock()
		return errors.New("ID already in ack")
	}
	t.inAckMessages[msg.ID] = msg
	t.inAckMutex.Unlock()
	return nil
}

func (t *Topic) popInAckMsg(id MessageID) (*Message, error) {
	t.inAckMutex.Lock()
	msg, ok := t.inAckMessages[id]
	if !ok {
		t.inAckMutex.Unlock()
		return nil, errors.New("ID not in ack")
	}

	delete(t.inAckMessages, id)
	t.inAckMutex.Unlock()
	return msg, nil
}

func (t *Topic) addToInAckQueue(msg *Message) {
	t.inAckMutex.Lock()
	t.inAckQ.Push(msg)
	t.inAckMutex.Unlock()
}

func (t *Topic) removeFromInAckQueue(msg *Message) {
	t.inAckMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the queue
		t.inAckMutex.Unlock()
		return
	}
	t.inAckQ.Remove(msg.index)
	t.inAckMutex.Unlock()
}

func (t *Topic) addToInAckDB(msg *Message) error {
	key := append([]byte(t.name), msg.ID[:]...)
	buf := bufferPoolGet()
	defer bufferPoolPut(buf)
	_, err := msg.WriteToAckDB(buf)
	if err != nil {
		return err
	}
	return t.server.pendingDB.Put(key, buf.Bytes(), nil)
}

//remove leveldb in ack msg
func (t *Topic) removeMsgInAckDB(key []byte) error {
	return t.server.pendingDB.Delete(key, nil)
}

func (t *Topic) processInAckQueue(time int64) {
	if t.Exiting() {
		return
	}
	for {
		t.inAckMutex.Lock()
		msg, _ := t.inAckQ.PeekAndShift(time)
		t.inAckMutex.Unlock()

		if msg == nil {
			return
		}

		_, err := t.popInAckMsg(msg.ID)
		if err != nil {
			return
		}

		err = t.put(msg)
		if err != nil {
			continue
		}
		key := append([]byte(t.name), msg.ID[:]...)
		t.removeMsgInAckDB(key)
	}
}

func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
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

	t.waitGroup.Wait()

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
