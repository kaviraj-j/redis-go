package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/parser"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Println("Listening on :6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Println("Handling new connection from", conn.RemoteAddr())
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		cmd, err := parser.ParseRequest(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Client disconnected:", conn.RemoteAddr())
				return
			}
			fmt.Println("Error parsing request:", err)
			return
		}
		if cmd == nil {
			continue
		}

		switch strings.ToUpper(cmd.Name) {
		case "PING":
			// PING -> +PONG
			conn.Write(parser.EncodeString("PONG"))

		case "ECHO":
			// ECHO message -> echo back
			if len(cmd.Args) < 1 {
				conn.Write(parser.EncodeString("ERR wrong number of arguments for 'echo' command"))
				continue
			}
			conn.Write(parser.EncodeBulkString(cmd.Args[0]))

		default:
			conn.Write(parser.EncodeString("ERR unknown command '" + cmd.Name + "'"))
		}
	}
}
