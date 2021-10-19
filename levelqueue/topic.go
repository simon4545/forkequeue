package levelqueue

import "sync"

type Topic struct {
	sync.RWMutex

	name     string
	backend  *levelQueue
	exitChan chan int
	exitFlag int32
}

func NewTopic(topicName string) {

}
