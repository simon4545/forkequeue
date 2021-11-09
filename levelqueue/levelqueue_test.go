package levelqueue

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

func BenchmarkQueuePush16(b *testing.B) {
	benchMarkQueuePush(16, b)
}

func BenchmarkQueuePush1024(b *testing.B) {
	benchMarkQueuePush(1024, b)
}

func BenchmarkQueuePush4096(b *testing.B) {
	benchMarkQueuePush(4096, b)
}

func BenchmarkQueuePush1048576(b *testing.B) {
	benchMarkQueuePush(1048576, b)
}

func benchMarkQueuePush(size int64, b *testing.B) {
	b.StopTimer()
	dbName := "bench_leveldb_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	dbPath := "./data"

	lq := NewQueue(dbName, dbPath)

	b.SetBytes(size)
	data := make([]byte, size)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		err := lq.Push(data)
		if err != nil {
			panic(err)
		}
	}

	err := lq.Close()
	if err != nil {
		panic(err)
	}
	err = os.RemoveAll(path.Join(dbPath, dbName))
	if err != nil {
		panic(err)
	}
}

func BenchmarkQueuePop16(b *testing.B) {
	benchMarkQueuePop(16, b)
}

func BenchmarkQueuePop1024(b *testing.B) {
	benchMarkQueuePop(1024, b)
}

func BenchmarkQueuePop4096(b *testing.B) {
	benchMarkQueuePop(4096, b)
}

func BenchmarkQueuePop1048576(b *testing.B) {
	benchMarkQueuePop(1048576, b)
}

func benchMarkQueuePop(size int64, b *testing.B) {
	b.StopTimer()
	dbName := "bench_leveldb_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	dbPath := "./data"

	lq := NewQueue(dbName, dbPath)

	b.SetBytes(size)
	data := make([]byte, size)

	for i := 0; i < b.N; i++ {
		err := lq.Push(data)
		if err != nil {
			panic(err)
		}
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		<-lq.Pop()
	}

	err := lq.Close()
	if err != nil {
		panic(err)
	}
	err = os.RemoveAll(path.Join(dbPath, dbName))
	if err != nil {
		panic(err)
	}
}

func BenchmarkDbPush16(b *testing.B) {
	benchMarkDbPut(16, b)
}

func benchMarkDbPut(size int64, b *testing.B) {
	b.StopTimer()

	dbName := "bench_leveldb_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	dbPath := "./data"

	filePath := path.Join(dbPath, dbName)
	ldb, err := leveldb.OpenFile(filePath, nil)
	if err != nil {
		panic(err)
	}

	b.SetBytes(size)
	data := make([]byte, size)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		err := ldb.Put([]byte(fmt.Sprintf("%d", i)), data, nil)
		if err != nil {
			panic(err)
		}
	}

	err = ldb.Close()
	if err != nil {
		panic(err)
	}
	err = os.RemoveAll(path.Join(dbPath, dbName))
	if err != nil {
		panic(err)
	}
}

func BenchmarkDbGet16(b *testing.B) {
	benchMarkDbGet(16, b)
}

func BenchmarkDbGet4096(b *testing.B) {
	benchMarkDbGet(4096, b)
}

func benchMarkDbGet(size int64, b *testing.B) {
	b.StopTimer()

	dbName := "bench_leveldb_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	dbPath := "./data"

	filePath := path.Join(dbPath, dbName)
	ldb, err := leveldb.OpenFile(filePath, nil)
	if err != nil {
		panic(err)
	}

	b.SetBytes(size)
	data := make([]byte, size)

	for i := 0; i < b.N; i++ {
		err := ldb.Put([]byte(fmt.Sprintf("%d", i)), data, nil)
		if err != nil {
			panic(err)
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := ldb.Get([]byte(fmt.Sprintf("%d", i)), nil)
		if err != nil {
			panic(err)
		}
	}

	err = ldb.Close()
	if err != nil {
		panic(err)
	}
	err = os.RemoveAll(path.Join(dbPath, dbName))
	if err != nil {
		panic(err)
	}
}
