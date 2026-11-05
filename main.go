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

func handleClient(conn net.Conn, h *Hub) {
	defer conn.Close()

	client := Client{ID: GenerateClientID(), Conn: conn, JoinTime: time.Now()}
	h.register <- &client

	_, err := conn.Write([]byte("Connected to localhost.\n"))
	if err != nil {
		fmt.Println(err)
		h.unregister <- &client
		return
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := ParseIncomingMessage(scanner.Text(), client.ID)
		h.broadcast <- msg
	}

	if scanner.Err() != nil {
		fmt.Println(scanner.Err())
	}
	h.unregister <- &client
}

func GenerateClientID() string {
	newId := int(lastUserId.Add(1))
	return "<User_" + strconv.Itoa(newId) + ">"
}

type Hub struct {
	clients    map[string]*Client
	broadcast  chan ChatMessage
	register   chan *Client
	unregister chan *Client
}

func (h *Hub) Run() {
	for {
		select {
		case msg := <-h.broadcast:
			h.BroadcastMessage(msg)
		case reg := <-h.register:
			h.clients[reg.ID] = reg
		case unreg := <-h.unregister:
			delete(h.clients, unreg.ID)
		}
	}
}

func (h *Hub) BroadcastMessage(msg ChatMessage) {
	for _, client := range h.clients {
		if client.ID == msg.ClientID {
			continue
		}

		client.Conn.Write([]byte(FormatMessage(msg) + "\n"))
	}
}

func main() {
	err := StartEchoServer(":8081")
	if err != nil {
		fmt.Println(err)
	}
}

func StartEchoServer(port string) error {
	hub := Hub{clients: make(map[string]*Client), broadcast: make(chan ChatMessage), register: make(chan *Client), unregister: make(chan *Client)}
	go hub.Run()

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

		go handleClient(conn, &hub)
	}

	return nil
}
