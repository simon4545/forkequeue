package levelqueue

import (
	"encoding/binary"
	"log"
	"path"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// levelQueue implements a filesystem backed FIFO queue
type levelQueue struct {
	readPos  uint64
	writePos uint64

	sync.RWMutex

	// leveldb name and dataPath
	name     string
	dataPath string

	exitFlag int32

	nextReadPos uint64

	ldb      *leveldb.DB
	wOptions *opt.WriteOptions
	rOptions *opt.ReadOptions

	// exposed via Pop()
	readChan chan []byte

	// internal channels
	exitChan chan int
	// exitSyncChan chan int
}

func NewQueue(name string, dataPath string) *levelQueue {
	l := levelQueue{
		name:     name,
		dataPath: dataPath,
		readChan: make(chan []byte),
		exitChan: make(chan int),
	}

	err := l.loadData()
	if err != nil {
		log.Fatalln("init leveldb err:", err)
	}

	go l.ioLoop()
	return &l
}

func (l *levelQueue) Push(data []byte) error {
	l.RLock()
	defer l.RUnlock()

	if l.exitFlag == 1 {
		return errors.New("exiting")
	}
	pos := make([]byte, 8)
	binary.BigEndian.PutUint64(pos, l.writePos)
	if err := l.ldb.Put(pos, data, l.wOptions); err != nil {
		return err
	}

	binary.BigEndian.PutUint64(pos, l.writePos+1)
	if err := l.ldb.Put([]byte("_writePosition"), pos, l.wOptions); err != nil {
		return err
	}
	l.writePos += 1
	return nil
}

func (l *levelQueue) Pop() <-chan []byte {
	return l.readChan
}

func (l *levelQueue) Close() error {
	l.Lock()
	defer l.Unlock()

	l.exitFlag = 1

	close(l.exitChan)
	<-l.exitChan
	log.Printf("QUEUE(%s): closing... ioLoop\n", l.name)

	if l.ldb != nil {
		return l.ldb.Close()
	}

	return nil
}

func (l *levelQueue) loadData() error {
	filePath := path.Join(l.dataPath, l.name)
	db, err := leveldb.OpenFile(filePath, nil)
	if err != nil {
		return err
	}

	var readPosition uint64 = 0
	var writePosition uint64 = 0

	readPos, err := db.Get([]byte("_readPosition"), nil)
	if err != nil && err != leveldb.ErrNotFound {
		return err
	} else if readPos != nil {
		readPosition = binary.BigEndian.Uint64(readPos)
	}

	writePos, err := db.Get([]byte("_writePosition"), nil)
	if err != nil && err != leveldb.ErrNotFound {
		return err
	} else if writePos != nil {
		writePosition = binary.BigEndian.Uint64(writePos)
	}

	l.ldb = db
	l.readPos = readPosition
	l.nextReadPos = l.readPos
	l.writePos = writePosition

	return nil
}

func (l *levelQueue) readOne() ([]byte, error) {
	pos := make([]byte, 8)
	binary.BigEndian.PutUint64(pos, l.readPos)
	value, err := l.ldb.Get(pos, l.rOptions)
	if err != nil {
		return nil, err
	}
	/*if value != nil {
		if err = l.ldb.Delete(pos, l.wOptions); err != nil {
			return nil, err
		}
	}*/

	l.nextReadPos = l.readPos + 1
	return value, nil
}

func (l *levelQueue) modifyPosition() {
	pos := make([]byte, 8)
	binary.BigEndian.PutUint64(pos, l.nextReadPos)
	err := l.ldb.Put([]byte("_readPosition"), pos, l.wOptions)
	if err != nil {
		log.Printf("QUEUE(%s)write read position(%d) err:%s", l.name, l.readPos, err)
	}

	binary.BigEndian.PutUint64(pos, l.readPos)
	err = l.ldb.Delete(pos, l.wOptions)
	if err != nil {
		log.Printf("QUEUE(%s)delete read position(%d) data err:%s", l.name, l.readPos, err)
	}

	l.readPos = l.nextReadPos
}

func (l *levelQueue) ioLoop() {
	var dataRead []byte
	// var r chan []byte
	var err error
	// go func() {
	// 	l.exitSyncChan <- 1
	// }()
	for {
		if l.readPos < l.writePos {
			dataRead, err = l.readOne()
			// if l.nextReadPos == l.readPos {
			if err != nil {
				continue
			}
			l.readChan <- dataRead
			l.modifyPosition()
			// }
			// r = l.readChan
		}
	}
}
