package main

import (
	"bufio"
	"fmt"
	"net"
)

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
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		conn.Write([]byte(scanner.Text() + "\n"))
	}
	return nil
}
