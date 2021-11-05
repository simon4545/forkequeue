package levelqueue

import (
	"sort"
	"sync/atomic"
)

type Stats struct {
	Topics []TopicStats
}

type TopicStats struct {
	TopicName    string `json:"topic_name"`
	MessageCount uint64 `json:"message_count"`
	InAckCount   int    `json:"in_ack_count"`
}

func NewTopicStats(t *Topic) TopicStats {
	t.RLock()
	read := atomic.LoadUint64(&(t.queue.readPos))
	write := atomic.LoadUint64(&(t.queue.writePos))
	inAck := len(t.inAckMessages)
	t.RUnlock()

	return TopicStats{
		TopicName:    t.name,
		MessageCount: write - read,
		InAckCount:   inAck,
	}
}

type Topics []*Topic

func (t Topics) Len() int      { return len(t) }
func (t Topics) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type TopicsByName struct {
	Topics
}

func (t TopicsByName) Less(i, j int) bool { return t.Topics[i].name < t.Topics[j].name }

func (s *Server) GetStats(topic string) Stats {
	var stats Stats

	s.RLock()
	var realTopics []*Topic
	if topic == "" {
		realTopics = make([]*Topic, 0, len(s.topicMap))
		for _, t := range s.topicMap {
			realTopics = append(realTopics, t)
		}
	} else if val, exists := s.topicMap[topic]; exists {
		realTopics = []*Topic{val}
	} else {
		s.RUnlock()
		return stats
	}
	s.RUnlock()
	sort.Sort(TopicsByName{realTopics})

	topics := make([]TopicStats, 0, len(realTopics))

	for _, t := range realTopics {
		topics = append(topics, NewTopicStats(t))
	}
	stats.Topics = topics

	return stats
}
