package levelqueue

import (
	"encoding/json"
	"fmt"
	"forkequeue/internal/response"
	"github.com/gin-gonic/gin"
	"net/http"
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
	return hs
}

type PushData struct {
	Data interface{} `json:"data" form:"data"`
}

func (hs *httpServer) pushHandler(c *gin.Context) {
	var pushData PushData
	if err := c.ShouldBindJSON(&pushData); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}

	b, err := json.Marshal(pushData)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}

	topic := hs.server.GetTopic("test")
	msg := NewMessage(topic.GenerateID(), b)
	err = topic.PutMessage(msg)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
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
	topic := hs.server.GetTopic("test")
	buf := <-topic.ReadChan()
	msg, err := decodeMessage(buf)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}

	var popData PopData
	if err := json.Unmarshal(msg.Body, &popData.Data); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	popData.ID = msg.ID

	fmt.Println(string(msg.ID[:]))
	response.OkWithData(popData, c)
	return
}
