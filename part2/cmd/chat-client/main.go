package main

import (
	"context"
	"log"
	"os"

	"chat/chatpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("usage: chat-client USER send TEXT | chat-client USER subscribe")
	}
	user, cmd := os.Args[1], os.Args[2]

	conn, err := grpc.NewClient("localhost:6000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	c := chatpb.NewChatRoomClient(conn)


	switch cmd {
	case "send": // unary
		r, err := c.Send(context.Background(), &chatpb.ChatMessage{User: user, Text: os.Args[3]})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("sent, seq=%d", r.Seq)

	case "subscribe": // server streaming
		stream, err := c.Subscribe(context.Background(), &chatpb.SubscribeRequest{User: user})
		if err != nil {
			log.Fatal(err)
		}
		log.Println("subscribed, waiting for messages...")
		for {
			m, err := stream.Recv()
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("[%d] %s: %s", m.Seq, m.User, m.Text)
		}
	}
}
