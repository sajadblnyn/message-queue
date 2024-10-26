package server

import "github.com/sajadblnyn/message-queue/queue"

type Topic struct {
	Name          string
	MQ            queue.IMessageQueue
	Subscriptions map[string]bool
}

func NewTopic(name string) *Topic {
	return &Topic{
		Name: name,
		MQ:   queue.NewMessageQueue(),
	}
}

func (t *Topic) GetMessageQueue() *queue.MessageQueue {
	return t.MQ.(*queue.MessageQueue)
}
