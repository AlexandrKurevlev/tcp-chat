package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

type ChatMessage struct {
	Timestamp   time.Time
	ClientID    string
	Content     string
	MessageType string
}

func FormatMessage(msg ChatMessage) string {
	if msg.MessageType == "user" {
		return fmt.Sprintf("[%v] %v: %v", msg.Timestamp.Format(time.TimeOnly), msg.ClientID, msg.Content)
	} else if msg.MessageType == "system" {
		return fmt.Sprintf("[%v] ***%v", msg.Timestamp.Format(time.TimeOnly), msg.Content)
	}

	return ""
}

func ParseIncomingMessage(raw string, senderID string) ChatMessage {
	return ChatMessage{Timestamp: time.Now(), ClientID: senderID, Content: raw, MessageType: "user"}
}

func main() {
	err := StartEchoServer(":8080")
	if err != nil {
		fmt.Println(err)
	}
}

func StartEchoServer(port string) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Println("TCP Echo Server listening on " + port)
	fmt.Println("Waiting for connection...")

	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.Write([]byte("Connected to localhost.\n"))

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := ParseIncomingMessage(scanner.Text(), "<User_001>")
		conn.Write([]byte(FormatMessage(msg) + "\n"))
	}
	return nil
}
