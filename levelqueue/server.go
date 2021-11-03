package levelqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"forkequeue/internal/util"
	"github.com/syndtr/goleveldb/leveldb"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Server struct {
	pendingDB *leveldb.DB
	sync.RWMutex

	opts atomic.Value

	topicMap   map[string]*Topic
	exitChan   chan int
	waitGroup  util.WaitGroupWrapper
	isLoading  int32
	isExiting  int32
	notifyChan chan interface{}
}

func (s *Server) getOpts() *Options {
	return s.opts.Load().(*Options)
}

func (s *Server) storeOpts(opts *Options) {
	s.opts.Store(opts)
}

type meta struct {
	Topics []struct {
		Name string `json:"name"`
	} `json:"topics"`
}

func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "server-topic.dat")
}

func readOrEmpty(fn string) ([]byte, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

func (s *Server) LoadMetadata() error {
	atomic.StoreInt32(&s.isLoading, 1)
	defer atomic.StoreInt32(&s.isLoading, 0)

	fn := newMetadataFile(s.getOpts())

	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // fresh start
	}

	var m meta
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	for _, t := range m.Topics {

		topic := s.GetTopic(t.Name)

		topic.Start()
	}
	return nil
}

func (s *Server) PersistMetadata() error {
	// persist metadata about what topics/channels we have, across restarts
	fileName := newMetadataFile(s.getOpts())

	jsonData := make(map[string]interface{})

	var topics []interface{}
	for _, topic := range s.topicMap {

		topicData := make(map[string]interface{})
		topicData["name"] = topic.name

		topics = append(topics, topicData)
	}

	jsonData["topics"] = topics

	data, err := json.Marshal(&jsonData)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) GetTopic(topicName string) *Topic {
	s.RLock()
	t, ok := s.topicMap[topicName]
	s.RUnlock()
	if ok {
		return t
	}

	s.Lock()

	t, ok = s.topicMap[topicName]
	if ok {
		s.Unlock()
		return t
	}

	t = NewTopic(topicName, s)
	s.topicMap[topicName] = t

	s.Unlock()

	if atomic.LoadInt32(&s.isLoading) == 1 {
		return t
	}

	t.Start()
	return t
}

func (s *Server) topics() []*Topic {
	var topics []*Topic
	s.RLock()
	for _, t := range s.topicMap {
		topics = append(topics, t)
	}
	s.RUnlock()
	return topics
}

func (s *Server) Notify(v interface{}) {
	loading := atomic.LoadInt32(&s.isLoading) == 1

	s.waitGroup.Wrap(func() {
		select {
		case <-s.exitChan:
		case s.notifyChan <- v:
			if loading {
				<-s.notifyChan
				return
			}
			s.Lock()
			err := s.PersistMetadata()
			if err != nil {
				log.Printf("failed to persist metadata - %s\n", err)
			}
			s.Unlock()
			<-s.notifyChan
		}
	})
}

func (s *Server) Exit() {
	if !atomic.CompareAndSwapInt32(&s.isExiting, 0, 1) {
		return
	}

	s.Lock()
	err := s.PersistMetadata()
	if err != nil {
		log.Printf("failed to persist metadata - %s\n", err)
	}
	log.Printf("closing topics\n")
	for _, topic := range s.topicMap {
		topic.Close()
	}
	s.Unlock()
	close(s.exitChan)
	s.waitGroup.Wait()
	s.pendingDB.Close()
}

func New(opts *Options) *Server {
	s := &Server{
		topicMap:   make(map[string]*Topic),
		exitChan:   make(chan int),
		notifyChan: make(chan interface{}, 1),
	}
	s.storeOpts(opts)
	return s
}

func (s *Server) InitPendingDB() error {
	filePath := path.Join(s.getOpts().DataPath, "topic-pending-msg")
	db, err := leveldb.OpenFile(filePath, nil)
	if err != nil {
		return err
	}
	s.pendingDB = db
	return nil
}

func (s *Server) Main() {
	httpServer := newHttpServer(s)
	hs := http.Server{Addr: s.getOpts().HTTPAddress, Handler: httpServer.router}

	go func() {
		if err := hs.ListenAndServe(); err != nil && errors.Is(err, http.ErrServerClosed) {
			log.Printf("listen: %s\n", err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down http server...")

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	//first shutdown http server
	err := hs.Shutdown(ctx)
	if err != nil {
		log.Println("HTTP Server forced to shutdown:", err)
	}
	log.Println("HTTP Server exiting")
	//second exit topic and queue
	s.Exit()

	log.Println("Server exiting")
}
