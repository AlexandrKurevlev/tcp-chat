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
	client := h.setupClientConnection(conn)
	h.register <- client

	h.handleClientMessages(client)
}

func GenerateClientID() string {
	newId := int(lastUserId.Add(1))
	return "<User_" + strconv.Itoa(newId) + ">"
}

type Request struct {
	Command  int
	Response chan any
}

type Hub struct {
	clients    map[string]*Client
	broadcast  chan ChatMessage
	register   chan *Client
	unregister chan *Client
	requests   chan Request
}

func (h *Hub) Run() {
	for {
		select {
		case req := <-h.requests:
			if req.Command == 1 {
				activeClients := make([]string, 0, len(h.clients))
				for k := range h.clients {
					activeClients = append(activeClients, k)
				}
				req.Response <- activeClients
			} else if req.Command == 2 {
				req.Response <- len(h.clients)
			}
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

		_, err := client.Conn.Write([]byte(FormatMessage(msg) + "\n"))
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (h *Hub) GetActiveClients() []string {
	req := Request{Command: 1, Response: make(chan any)}
	h.requests <- req
	return (<-req.Response).([]string)
}

func (h *Hub) GetClientCount() int {
	req := Request{Command: 2, Response: make(chan any)}
	h.requests <- req
	return (<-req.Response).(int)
}

func (h *Hub) setupClientConnection(conn net.Conn) *Client {
	client := Client{ID: GenerateClientID(), Conn: conn, JoinTime: time.Now()}
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	conn.Write([]byte("Welcome," + client.ID + "\n"))
	return &client
}

func (h *Hub) cleanupClient(client *Client) {
	defer client.Conn.Close()

	h.unregister <- client

	msg := ChatMessage{Timestamp: time.Now(), Content: client.ID + " has left the chat", MessageType: "system"}
	h.broadcast <- msg
}

func (h *Hub) handleClientMessages(client *Client) {
	defer h.cleanupClient(client)

	scanner := bufio.NewScanner(client.Conn)
	for scanner.Scan() {
		msg := ParseIncomingMessage(scanner.Text(), client.ID)
		h.broadcast <- msg

		client.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
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
