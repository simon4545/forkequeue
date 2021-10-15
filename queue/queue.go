package queue

import (
	"encoding/binary"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"sync"
)

type LevelQueue struct {
	Db            *leveldb.DB
	mutex         sync.Mutex
	cond          *sync.Cond
	readPosition  uint64
	writePosition uint64
	woptions      *opt.WriteOptions
	roptions      *opt.ReadOptions
}

func CreateQueue(dbpath string) (*LevelQueue, error) {
	//options := &opt.Options{}

	woptions := &opt.WriteOptions{}
	roptions := &opt.ReadOptions{}

	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		return nil, err
	}

	var readPosition uint64 = 0
	var writePosition uint64 = 0

	readBuffs, err := db.Get([]byte("_readPosition"), nil)
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	} else if readBuffs != nil {
		readPosition = binary.BigEndian.Uint64(readBuffs)
	}
	writeBuffs, err := db.Get([]byte("_writePosition"), nil)
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	} else if writeBuffs != nil {
		writePosition = binary.BigEndian.Uint64(writeBuffs)
	}

	q := LevelQueue{
		Db:            db,
		readPosition:  readPosition,
		writePosition: writePosition,
		woptions:      woptions,
		roptions:      roptions,
	}
	q.cond = sync.NewCond(&q.mutex)
	return &q, nil
}

func (q *LevelQueue) Push(data []byte) (bool, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	pos := make([]byte, 8)
	binary.BigEndian.PutUint64(pos, q.writePosition)
	if err := q.Db.Put(pos, data, q.woptions); err != nil {
		return false, err
	}

	binary.BigEndian.PutUint64(pos, q.writePosition+1)
	if err := q.Db.Put([]byte("_writePosition"), pos, q.woptions); err != nil {
		return false, err
	}
	q.writePosition = q.writePosition + 1
	q.cond.Signal()
	return true, nil
}

func (q *LevelQueue) Pop() ([]byte, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for q.readPosition >= q.writePosition {
		q.cond.Wait()
	}

	fmt.Println("pop read pos is :", q.readPosition)

	pos := make([]byte, 8)
	binary.BigEndian.PutUint64(pos, q.readPosition)
	value, err := q.Db.Get(pos, q.roptions)
	if err != nil {
		return nil, err
	}
	if value != nil {
		if err = q.Db.Delete(pos, q.woptions); err != nil {
			return nil, err
		}
	}
	binary.BigEndian.PutUint64(pos, q.readPosition+1)
	err = q.Db.Put([]byte("_readPosition"), pos, q.woptions)
	if err != nil {
		return nil, err
	}
	q.readPosition = q.readPosition + 1
	return value, nil
}

func (q *LevelQueue) GetReadPosition() uint64 {
	return q.readPosition
}

func (q *LevelQueue) GetWritePosition() uint64 {
	return q.writePosition
}

func (q *LevelQueue) CloseQueue() {
	q.Db.Close()
}

func (q *LevelQueue) Stats() string {
	stats, _ := q.Db.GetProperty("leveldb.stats")
	return stats
}
