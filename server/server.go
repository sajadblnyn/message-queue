package server

import (
	"encoding/json"
	"log"
	"net"
	"sync"

	"github.com/google/uuid"
	"github.com/sajadblnyn/message-queue/queue"
)

type Server struct {
	Addr     string
	Clients  map[string]net.Conn
	Topics   map[string]*Topic
	listener net.Listener
	mu       sync.Mutex
	actions  map[string]func(net.Conn, *Request)
}

type Message struct {
	Topic    string `json:"topic"`
	Content  string `json:"content"`
	Priority int    `json:"priority"`
}

type Request struct {
	Action  string  `json:"action"`
	Topic   string  `json:"topic"`
	Message Message `json:"message"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type ResponseMessage struct {
	MessageId string `json:"message_id"`
	Topic     string `json:"topic"`
	Content   string `json:"content"`
	Priority  int    `json:"priority"`
}

type DeliverMessage struct {
	Action  string          `json:"action"`
	Message ResponseMessage `json:"message"`
}

type SuccessResponse struct {
	Status string `json:"status"`
}

func NewServer(address string) *Server {
	server := &Server{
		Addr:    address,
		Clients: map[string]net.Conn{},
		Topics:  make(map[string]*Topic),
		mu:      sync.Mutex{},
	}

	server.actions = map[string]func(net.Conn, *Request){
		"publish":          server.publicMessage,
		"subscribe":        server.subscribe,
		"unsubscribe":      server.unsubscribe,
		"shutdown":         server.shutdown,
		"close_connection": server.closeConnection,
	}

	return server
}

func (s *Server) publicMessage(conn net.Conn, r *Request) {
	encoder := json.NewEncoder(conn)

	if r.Message.Content == "" {
		encoder.Encode(&ErrorResponse{
			Error: "message content is required",
		})
	}

	if r.Message.Topic == "" {
		encoder.Encode(&ErrorResponse{
			Error: "topic is required",
		})
	}

	if r.Message.Priority == 0 {
		encoder.Encode(&ErrorResponse{
			Error: "priority is required",
		})
	}

	t, e := s.GetTopic(r.Message.Topic)

	m := queue.Message{
		ID:       uuid.New(),
		Content:  r.Message.Content,
		Priority: r.Message.Priority,
	}
	t.MQ.Push(&m)

	if !e {
		go s.runTopicFlow(t)
	}

	encoder.Encode(&SuccessResponse{Status: "ok"})

}

func (s *Server) runTopicFlow(t *Topic) {
	for {
		if t.MQ.Len() < 1 {
			continue
		}
		m := t.MQ.Pop().(*queue.Message)
		if m != nil {
			for k, _ := range t.Subscriptions {
				s.mu.Lock()
				conn := s.Clients[k]
				s.mu.Unlock()

				encoder := json.NewEncoder(conn)
				encoder.Encode(&DeliverMessage{
					Action:  "deliver",
					Message: ResponseMessage{MessageId: m.ID.String(), Topic: t.Name, Content: m.Content, Priority: m.Priority},
				})

			}
		}

	}
}

func (s *Server) closeConnection(conn net.Conn, r *Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.Clients, conn.RemoteAddr().String())
	conn.Close()

	encoder := json.NewEncoder(conn)
	encoder.Encode(&SuccessResponse{Status: "ok"})

}
func (s *Server) shutdown(conn net.Conn, r *Request) {
	encoder := json.NewEncoder(conn)
	s.Stop()
	encoder.Encode(&SuccessResponse{Status: "ok"})

}
func (s *Server) subscribe(conn net.Conn, r *Request) {
	encoder := json.NewEncoder(conn)

	if r.Topic == "" {
		encoder.Encode(&ErrorResponse{
			Error: "topic is required",
		})
	}

	t, e := s.GetTopic(r.Topic)
	s.mu.Lock()
	s.Topics[t.Name].Subscriptions[conn.RemoteAddr().String()] = true
	s.mu.Unlock()

	if !e {
		go s.runTopicFlow(t)
	}
	encoder.Encode(&SuccessResponse{Status: "ok"})

}
func (s *Server) unsubscribe(conn net.Conn, r *Request) {
	encoder := json.NewEncoder(conn)

	if r.Topic == "" {
		encoder.Encode(&ErrorResponse{
			Error: "topic is required",
		})
	}

	t, _ := s.GetTopic(r.Topic)
	s.mu.Lock()
	delete(s.Topics[t.Name].Subscriptions, conn.RemoteAddr().String())
	s.mu.Unlock()

	encoder.Encode(&SuccessResponse{Status: "ok"})

}

func (s *Server) Run() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.listener = ln
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		s.mu.Lock()
		s.Clients[conn.RemoteAddr().String()] = conn
		s.mu.Unlock()
		go s.handler(conn)
	}

}

func (s *Server) handler(conn net.Conn) {
	for {
		decoder := json.NewDecoder(conn)
		req := &Request{}
		decoder.Decode(&req)

		action, e := s.actions[req.Action]
		if !e {
			encoder := json.NewEncoder(conn)
			res := ErrorResponse{Error: "unknown action"}
			encoder.Encode(&res)
			continue
		}
		action(conn, req)

	}

}
func (s *Server) Stop() {
	defer s.listener.Close()
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, v := range s.Clients {
		v.Close()
	}
	s.Clients = map[string]net.Conn{}
}

func (s *Server) GetTopic(topicName string) (*Topic, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, e := s.Topics[topicName]
	if e {
		return t, e
	}
	s.Topics[topicName] = &Topic{Name: topicName, MQ: queue.NewMessageQueue(), Subscriptions: map[string]bool{}}
	return s.Topics[topicName], false
}

func (s *Server) GetClientConnections() []net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	conns := []net.Conn{}
	for _, v := range s.Clients {
		conns = append(conns, v)
	}
	return conns
}
