package main

import (
	"context"
	"log"
	"net"
	"sync"

	"chat/chatpb"

	"google.golang.org/grpc"
)

type server struct {
	chatpb.UnimplementedChatRoomServer
	mu   sync.Mutex
	seq  int64
	subs map[chan *chatpb.ChatMessage]bool 
}

// Send (unary): number the message and hand it to every subscriber.
func (s *server) Send(ctx context.Context, m *chatpb.ChatMessage) (*chatpb.SendReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	m.Seq = s.seq
	for ch := range s.subs { 
		select {
		case ch <- m:
		default: 
		}
	}
	log.Printf("Send: seq=%d %s: %s", m.Seq, m.User, m.Text)
	return &chatpb.SendReply{Seq: m.Seq}, nil
}

// Subscribe (server streaming): push every new message until the client leaves.
func (s *server) Subscribe(req *chatpb.SubscribeRequest, stream chatpb.ChatRoom_SubscribeServer) error {
	ch := make(chan *chatpb.ChatMessage, 16)
	s.mu.Lock()
	s.subs[ch] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.subs, ch)
		s.mu.Unlock()
	}()
	log.Printf("Subscribe: %s joined", req.User)

	for {
		select {
		case m := <-ch:
			if err := stream.Send(m); err != nil {
				return err
			}
		case <-stream.Context().Done():
			log.Printf("Subscribe: %s left", req.User)
			return nil
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", ":6000")
	if err != nil {
		log.Fatal(err)
	}
	g := grpc.NewServer()
	chatpb.RegisterChatRoomServer(g, &server{subs: map[chan *chatpb.ChatMessage]bool{}})
	log.Println("chat-server listening on :6000")
	log.Fatal(g.Serve(lis))
}