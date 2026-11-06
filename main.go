package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
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
	defer func() {
		if r := recover(); r != nil {
			h.cfg.logger.Printf("recovered: %v", r)
		}
	}()

	client := h.setupClientConnection(conn)
	h.register <- client

	h.cfg.logger.Println("Client " + client.ID + " connected")
	h.cfg.serverStats.ActiveConnections++

	recentMsgs := h.messageHistory.GetRecent()
	if len(recentMsgs) > 0 {
		conn.Write([]byte("--- Recent Messages ---\n"))
	}
	for _, msg := range recentMsgs {
		conn.Write([]byte(FormatMessage(msg) + "\n"))
	}
	if len(recentMsgs) > 0 {
		conn.Write([]byte("--- End of history ---\n"))
	}

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
	clients        map[string]*Client
	broadcast      chan ChatMessage
	register       chan *Client
	unregister     chan *Client
	requests       chan Request
	messageHistory *MessageHistory
	cfg            *Config
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
			log.Println(err)
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

func (h *Hub) SendUserList(client *Client) {
	clients := h.GetActiveClients()
	msg := fmt.Sprintf("Online users (%d): %s", len(clients), strings.Join(clients, ", "))
	client.Conn.Write([]byte(msg + "\n"))
}

func (h *Hub) SendHelpMessage(client *Client) {
	client.Conn.Write([]byte(
		"Available commands:\n" +
			"/help      - Show this help message\n" +
			"/users     - List online users\n" +
			"/time      - Show server time\n" +
			"/quit      - Disconnect from the chat\n"))
}

func (h *Hub) SendTimeMessage(client *Client) {
	client.Conn.Write([]byte("Server time: " + time.Now().Format(time.TimeOnly)))
}

func (h *Hub) HandleCommand(client *Client, command string) {
	switch command {
	case "/users":
		h.SendUserList(client)
	case "/help":
		h.SendHelpMessage(client)
	case "/time":
		h.SendTimeMessage(client)
	case "/quit":
		h.cleanupClient(client)
	default:
		client.Conn.Write([]byte("Unknown command\n"))
	}
}

func (h *Hub) Shutdown(ctx context.Context) error {
	h.cfg.logger.Println("Server is shutting down")
	<-ctx.Done()

	h.broadcast <- ChatMessage{Timestamp: time.Now(), Content: "Server is shutting down", MessageType: "system"}
	close(h.broadcast)
	for _, client := range h.clients {
		h.unregister <- client
		client.Conn.Close()
	}

	return nil
}

func (h *Hub) setupClientConnection(conn net.Conn) *Client {
	client := Client{ID: GenerateClientID(), Conn: conn, JoinTime: time.Now()}
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	conn.Write([]byte("Welcome," + client.ID + "\n"))
	return &client
}

func (h *Hub) cleanupClient(client *Client) {
	defer client.Conn.Close()

	if _, ok := h.clients[client.ID]; !ok {
		return
	}

	h.unregister <- client

	msg := ChatMessage{Timestamp: time.Now(), Content: client.ID + " has left the chat", MessageType: "system"}
	h.broadcast <- msg

	h.cfg.logger.Println("Cleaning up " + client.ID)
	h.cfg.serverStats.ActiveConnections--
}

func (h *Hub) handleClientMessages(client *Client) {
	defer h.cleanupClient(client)

	scanner := bufio.NewScanner(client.Conn)
	for scanner.Scan() {
		msg := ParseIncomingMessage(scanner.Text(), client.ID)
		h.cfg.serverStats.TotalMessagesProcessed++
		if strings.HasPrefix(msg.Content, "/") {
			h.HandleCommand(client, msg.Content)
		} else {
			h.messageHistory.Add(msg)
			h.broadcast <- msg
		}

		client.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	}
}

const defaultMessageHistorySize = 50

type MessageHistory struct {
	messages []ChatMessage
	head     int
}

func NewMessageHistory() *MessageHistory {
	return &MessageHistory{messages: make([]ChatMessage, 0, defaultMessageHistorySize), head: 0}
}

func (mh *MessageHistory) Add(msg ChatMessage) {
	if len(mh.messages) < defaultMessageHistorySize {
		mh.messages = append(mh.messages, msg)
	} else {
		mh.messages[mh.head] = msg
	}

	mh.head = (mh.head + 1) % defaultMessageHistorySize
}

func (mh *MessageHistory) GetRecent() []ChatMessage {
	res := make([]ChatMessage, 0, len(mh.messages))
	if len(mh.messages) < defaultMessageHistorySize {
		for _, msg := range mh.messages {
			res = append(res, msg)
		}
	} else {
		for i := range defaultMessageHistorySize {
			res = append(res, mh.messages[(mh.head+i)%defaultMessageHistorySize])
		}
	}
	return res
}

type ServerStats struct {
	ActiveConnections      int
	TotalMessagesProcessed int64
	UptimeSeconds          int64
	ErrorCount             int
}

func setupLogging(level string) *log.Logger {
	return log.New(os.Stdout, "[TCP-CHAT] ", log.Ldate|log.Ltime)
}

type Config struct {
	logger      *log.Logger
	serverStats *ServerStats
}

func setupSignalHandling() chan os.Signal {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	return c
}

func main() {
	cfg := &Config{logger: setupLogging(""), serverStats: &ServerStats{}}

	err := StartEchoServer(cfg, ":8081")
	if err != nil {
		fmt.Println(err)
	}
}

func StartEchoServer(cfg *Config, port string) error {
	hub := Hub{
		clients:        make(map[string]*Client),
		broadcast:      make(chan ChatMessage),
		register:       make(chan *Client),
		unregister:     make(chan *Client),
		requests:       make(chan Request),
		messageHistory: NewMessageHistory(),
		cfg:            cfg,
	}
	go hub.Run()

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := setupSignalHandling()

	go func() {
		<-sigChan
		cancel()
		hub.Shutdown(ctx)
		os.Exit(0)
	}()

	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	defer listener.Close()
	cfg.logger.Println("TCP Echo Server listening on " + port)
	cfg.logger.Println("Waiting for connection...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go handleClient(conn, &hub)
	}

	return nil
}
