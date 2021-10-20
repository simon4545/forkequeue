package api

import (
	"encoding/json"
	queue2 "forkequeue/queue"
	"github.com/gin-gonic/gin"
	"sync"
	"time"
)

var queue *queue2.LevelQueue

func NewRouter(q *queue2.LevelQueue) *gin.Engine {
	queue = q
	router := gin.Default()

	pubGroup := router.Group("api/queue")

	pubGroup.POST("push", pushHandler)
	pubGroup.POST("pop", popHandler)
	pubGroup.POST("test-push", testPush)
	pubGroup.POST("test-pop", testPop)

	t = &Test{}

	return router
}

type Test struct {
	mutex sync.Mutex
	name  string
}

var t *Test

func testPush(c *gin.Context) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	OkWithMessage("push success", c)
}

func testPop(c *gin.Context) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	time.Sleep(5 * time.Second)
	OkWithMessage("pop success", c)
}

type PushData struct {
	Data interface{} `json:"data" form:"data"`
}

func pushHandler(c *gin.Context) {
	var pushData PushData
	if err := c.ShouldBindJSON(&pushData); err != nil {
		FailWithMessage(err.Error(), c)
		return
	}

	b, err := json.Marshal(pushData)
	if err != nil {
		FailWithMessage(err.Error(), c)
		return
	}

	_, err = queue.Push(b)
	if err != nil {
		FailWithMessage(err.Error(), c)
		return
	}

	Ok(c)
	return
}

func popHandler(c *gin.Context) {
	data, err := queue.Pop()
	if err != nil {
		FailWithMessage(err.Error(), c)
		return
	}
	var popData PushData
	if err := json.Unmarshal(data, &popData); err != nil {
		FailWithMessage(err.Error(), c)
		return
	}

	OkWithData(popData.Data, c)
	return
}
