package queue

import (
	"container/heap"

	"github.com/google/uuid"
)

type IMessageQueue interface {
	heap.Interface
}

type Message struct {
	ID       uuid.UUID
	Content  string
	Priority int
	Index    int
	// other fields
}

type MessageQueue []*Message

func (pq MessageQueue) Len() int { return len(pq) }

func (pq MessageQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}
func (pq MessageQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *MessageQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Message)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *MessageQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.Index = -1
	*pq = old[0 : n-1]
	return item
}

func NewMessageQueue() IMessageQueue {
	mq := &MessageQueue{}
	heap.Init(mq)
	return mq
}
