package levelqueue

import (
	"context"
	"encoding/json"
	"forkequeue/internal/response"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"time"
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
	return hs
}

type PushData struct {
	Data interface{} `json:"data" form:"data" binding:"required"`
}

func (hs *httpServer) pushHandler(c *gin.Context) {
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

	topic := hs.server.GetTopic("test")
	msg := NewMessage(topic.GenerateID(), b)
	err = topic.PutMessage(msg)
	if err != nil {
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
	var msg *Message
	var buf []byte
	var err error
	topic := hs.server.GetTopic("test")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		response.FailWithMessage("pop timeout", c)
		return
	case buf = <-topic.ReadChan():
		msg, err = decodeMessage(buf)
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
	var finAckData FinishAckData
	if err := c.ShouldBindJSON(&finAckData); err != nil {
		log.Printf("err:%s\n", err.Error())
		response.FailWithMessage("msg id not exist", c)
		return
	}
	//log.Println("msg id :", finAckData.ID)
	topic := hs.server.GetTopic("test")
	err := topic.FinishMessage(finAckData.ID)

	if err != nil {
		log.Printf("topic(%s) err:%s\n", topic.name, err.Error())
		response.FailWithMessage("finish ack error", c)
		return
	}

	response.Ok(c)
	return
}
