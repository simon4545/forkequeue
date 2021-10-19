package levelqueue

import (
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"log"
	"path"
	"sync"
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
	needSync bool

	nextReadPos uint64

	ldb      *leveldb.DB
	wOptions *opt.WriteOptions
	rOptions *opt.ReadOptions

	// exposed via ReadChan()
	readChan chan []byte

	// internal channels
	writeChan         chan []byte
	writeResponseChan chan error
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int
}

func New(name string, dataPath string) *levelQueue {
	l := levelQueue{
		name:              name,
		dataPath:          dataPath,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
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

	l.writeChan <- data
	return <-l.writeResponseChan
}

func (l *levelQueue) Pop() <-chan []byte {
	return l.readChan
}

func (l *levelQueue) Close() error {
	l.Lock()
	defer l.Unlock()

	l.exitFlag = 1

	close(l.exitChan)
	<-l.exitSyncChan

	return nil
}

func (l *levelQueue) loadData() error {
	filePath := l.ldbFilePath()
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

func (l *levelQueue) ldbFilePath() string {
	return path.Join(l.dataPath, l.name)
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
	l.readPos = l.nextReadPos
}

func (l *levelQueue) writeOne(data []byte) error {
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

func (l *levelQueue) ioLoop() {
	var dataRead []byte
	var r chan []byte
	var err error

	for {

		if l.readPos < l.writePos {
			if l.nextReadPos == l.readPos {
				dataRead, err = l.readOne()
				if err != nil {
					continue
				}
			}
			r = l.readChan
		} else {
			r = nil
		}

		select {
		case r <- dataRead:
			l.modifyPosition()
		case dataWrite := <-l.writeChan:
			l.writeResponseChan <- l.writeOne(dataWrite)
		case <-l.exitChan:
			goto exit
		}

	}

exit:
	log.Printf("QUEUE(%s): closing... ioLoop\n", l.name)
	l.exitSyncChan <- 1
}
