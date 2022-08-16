package queue

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/simon4545/forkequeue/levelqueue"
)

type StatData struct {
	StartTime int64                   `json:"start_time"`
	Topics    []levelqueue.TopicStats `json:"topics"`
}

type FinishAckData struct {
	ID levelqueue.MessageID `json:"id" form:"id"`
}
type PushData struct {
	Data interface{} `json:"data" form:"data"`
}
type PopData struct {
	ID   levelqueue.MessageID `json:"id" form:"id"`
	Data interface{}          `json:"data" form:"data"`
}

var server *levelqueue.Server

func Init(dbPath string) bool {
	rand.Seed(time.Now().UnixNano())
	opts := levelqueue.NewOptions()
	opts.DataPath = dbPath
	server = levelqueue.New(opts)

	err := server.InitPendingDB()
	if err != nil {
		log.Fatalf("failed to init pendingDB - %s\n", err)
	}

	err = server.InitCheckSameDB()
	if err != nil {
		log.Fatalf("failed to init checkSameDB - %s\n", err)
	}

	err = server.LoadMetadata()
	if err != nil {
		log.Fatalf("failed to load metadata - %s\n", err)
	}

	err = server.PersistMetadata()
	if err != nil {
		log.Fatalf("failed to persist metadata - %s\n", err)
	}
	if err != nil {
		return false
	}
	return true
}
func Close() {
	server.Close()
}
func Pop(topicName string) (*PopData, error) {
	if topicName == "" || topicName == "topic-pending-msg" {
		log.Fatalf("topic name error")
		return nil, errors.New("topic name error")
	}

	var msg *levelqueue.Message
	var buf []byte
	var err error
	topic := server.GetTopic(topicName)

	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()
	timer := time.NewTimer(time.Second * 5)
	// timeAfterTrigger := time.After(time.Second * 5)

	select {
	case <-timer.C:
		// log.Panicln("pop timeout")
		timer.Stop()
		return nil, errors.New("pop timeout")
	case buf = <-topic.ReadChan():
		msg, err = levelqueue.DecodeMessage(buf)
		if err != nil {
			log.Fatalf("decode queue msg error")
			return nil, errors.New("decode queue msg error")
		}

		err = topic.StartInAckTimeOut(msg, 60*time.Second)
		if err != nil {
			log.Printf("topic(%s) err:%s\n", topic.Name(), err.Error())
		}
	}
	timer.Stop()
	var popData PopData
	var jsData PushData
	if err := json.Unmarshal(msg.Body, &jsData); err != nil {
		log.Printf("topic(%s) err:%s\n", topic.Name(), err.Error())
		return nil, errors.New("Unmarshal error")
	}
	popData.ID = msg.ID
	popData.Data = jsData.Data

	return &popData, nil
}
func Push(topicName string, pushData interface{}) bool {
	if topicName == "" || topicName == "topic-pending-msg" {
		log.Fatalf("failed to persist metadata \n")
		return false
	}

	b, err := json.Marshal(pushData)
	if err != nil {
		log.Fatalf("encoding msg error")
		return false
	}

	topic := server.GetTopic(topicName)

	hash := md5.Sum(b)
	key := append([]byte(topicName), hash[:]...)
	if topic.CheckIsExistSameMsg(key) {
		return false
	}

	msg := levelqueue.NewMessage(topic.GenerateID(), b)
	err = topic.PutMessage(msg)
	if err != nil {
		topic.DeleteSameMsg(key)
		log.Printf("topic(%s) err:%s\n", topic.Name(), err.Error())
		return false
	}

	return true
}

func Stats(topicName string) (*StatData, error) {
	if topicName == "topic-pending-msg" {
		log.Fatalf("topic name error")
		return nil, errors.New("topic name error")
	}

	stats := server.GetStats(topicName)
	startTime := server.GetStartTime()

	data := StatData{StartTime: startTime.Unix(), Topics: stats.Topics}
	return &data, nil
}

func Ack(topicName string, finAckData FinishAckData) bool {
	if topicName == "" || topicName == "topic-pending-msg" {
		log.Fatalf("topic name error")
		return false
	}

	topic := server.GetTopic(topicName)
	err := topic.FinishMessage(finAckData.ID)

	if err != nil {
		log.Printf("topic(%s) err:%s\n", topic.Name(), err.Error())
		return false
	}

	return true
}
