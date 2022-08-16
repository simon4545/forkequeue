package levelqueue

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/simon4545/forkequeue/response"

	"github.com/gin-gonic/gin"
)

type httpServer struct {
	server *Server
	router http.Handler
}

func newHttpServer(server *Server) *httpServer {
	router := gin.Default()

	hs := &httpServer{
		server: server,
		router: router,
	}

	pubGroup := router.Group("api/queue")

	pubGroup.POST("push", hs.pushHandler)
	pubGroup.POST("pop", hs.popHandler)
	pubGroup.POST("ack", hs.finishHandler)
	pubGroup.POST("stats", hs.StatsHandler)
	return hs
}

type PushData struct {
	Data interface{} `json:"data" form:"data" binding:"required"`
}

func (hs *httpServer) pushHandler(c *gin.Context) {
	topicName := c.Query("topic")
	if topicName == "" || topicName == "topic-pending-msg" {
		response.FailWithMessage("topic name error", c)
		return
	}

	var pushData PushData
	if err := c.ShouldBindJSON(&pushData); err != nil {
		log.Printf("err:%s\n", err.Error())
		response.FailWithMessage("msg not exist", c)
		return
	}

	b, err := json.Marshal(pushData)
	if err != nil {
		response.FailWithMessage("encoding msg error", c)
		return
	}

	topic := hs.server.GetTopic(topicName)

	hash := md5.Sum(b)
	key := append([]byte(topicName), hash[:]...)
	if topic.CheckIsExistSameMsg(key) {
		response.Ok(c)
		return
	}

	msg := NewMessage(topic.GenerateID(), b)
	err = topic.PutMessage(msg)
	if err != nil {
		topic.DeleteSameMsg(key)
		log.Printf("topic(%s) err:%s\n", topic.name, err.Error())
		response.FailWithMessage("put msg error", c)
		return
	}

	response.Ok(c)
	return
}

type PopData struct {
	ID   MessageID   `json:"id" form:"id"`
	Data interface{} `json:"data" form:"data"`
}

func (hs *httpServer) popHandler(c *gin.Context) {
	topicName := c.Query("topic")
	if topicName == "" || topicName == "topic-pending-msg" {
		response.FailWithMessage("topic name error", c)
		return
	}

	var msg *Message
	var buf []byte
	var err error
	topic := hs.server.GetTopic(topicName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		response.FailWithMessage("pop timeout", c)
		return
	case buf = <-topic.ReadChan():
		msg, err = DecodeMessage(buf)
		if err != nil {
			response.FailWithMessage("decode queue msg error", c)
			return
		}

		err = topic.StartInAckTimeOut(msg, 60*time.Second)
		if err != nil {
			log.Printf("topic(%s) err:%s\n", topic.name, err.Error())
		}
	}

	var popData PopData
	var jsData PushData
	if err := json.Unmarshal(msg.Body, &jsData); err != nil {
		log.Printf("topic(%s) err:%s\n", topic.name, err.Error())
		response.FailWithMessage("decode msg error", c)
		return
	}
	popData.ID = msg.ID
	popData.Data = jsData.Data

	//fmt.Println(string(msg.ID[:]))
	response.OkWithData(popData, c)
	return
}

type FinishAckData struct {
	ID MessageID `json:"id" form:"id" binding:"required"`
}

func (hs *httpServer) finishHandler(c *gin.Context) {
	topicName := c.Query("topic")
	if topicName == "" || topicName == "topic-pending-msg" {
		response.FailWithMessage("topic name error", c)
		return
	}

	var finAckData FinishAckData
	if err := c.ShouldBindJSON(&finAckData); err != nil {
		log.Printf("err:%s\n", err.Error())
		response.FailWithMessage("msg id not exist", c)
		return
	}
	//log.Println("msg id :", finAckData.ID)
	topic := hs.server.GetTopic(topicName)
	err := topic.FinishMessage(finAckData.ID)

	if err != nil {
		log.Printf("topic(%s) err:%s\n", topic.name, err.Error())
		response.FailWithMessage("finish ack error", c)
		return
	}

	response.Ok(c)
	return
}

func (hs *httpServer) StatsHandler(c *gin.Context) {
	topicName := c.Query("topic")
	if topicName == "topic-pending-msg" {
		response.FailWithMessage("topic name error", c)
		return
	}

	stats := hs.server.GetStats(topicName)
	startTime := hs.server.GetStartTime()

	data := struct {
		StartTime int64        `json:"start_time"`
		Topics    []TopicStats `json:"topics"`
	}{StartTime: startTime.Unix(), Topics: stats.Topics}

	response.OkWithData(data, c)
	return
}
