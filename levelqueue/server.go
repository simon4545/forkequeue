package levelqueue

import (
	"encoding/json"
	"fmt"
	"forkequeue/internal/util"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

type Server struct {
	sync.RWMutex
	topicMap  map[string]*Topic
	exitChan  chan int
	waitGroup util.WaitGroupWrapper
	isLoading int32
	isExiting int32
}

type meta struct {
	Topics []struct {
		Name string `json:"name"`
	} `json:"topics"`
}

func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
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
