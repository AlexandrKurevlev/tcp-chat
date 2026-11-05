package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

var lastUserId atomic.Int32

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
		return fmt.Sprintf("[%v] *** %v", msg.Timestamp.Format(time.TimeOnly), msg.Content)
	}

	return ""
}

func ParseIncomingMessage(raw string, senderID string) ChatMessage {
	return ChatMessage{Timestamp: time.Now(), ClientID: senderID, Content: raw, MessageType: "user"}
}

type Client struct {
	ID       string
	Conn     net.Conn
	JoinTime time.Time
}

func handleClient(conn net.Conn, clientID string) {
	defer conn.Close()

	_, err := conn.Write([]byte("Connected to localhost.\n"))
	if err != nil {
		fmt.Println(err)
		return
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := ParseIncomingMessage(scanner.Text(), clientID)
		_, err = conn.Write([]byte(FormatMessage(msg) + "\n"))
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func GenerateClientID() string {
	newId := int(lastUserId.Add(1))
	return "<User_" + strconv.Itoa(newId) + ">"
}

func main() {
	err := StartEchoServer(":8081")
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

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go handleClient(conn, GenerateClientID())
	}

	return nil
}
