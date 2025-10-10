package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

type App struct {
	store *store.Store
}

func newApp() *App {
	s, _ := store.NewStore()
	return &App{
		store: s,
	}
}

func (app *App) handleConnection(conn net.Conn) {
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
				conn.Write(parser.EncodeBulkString("ERR wrong number of arguments for 'echo' command"))
				continue
			}
			conn.Write(parser.EncodeBulkString(cmd.Args[0]))
		case "SET":
			// Set value and return OK
			if len(cmd.Args) < 2 {
				conn.Write(parser.EncodeBulkString("ERR wrong number of arguments for 'set' command"))
				continue
			}
			key, val := cmd.Args[0], cmd.Args[1]
			var expiry time.Time
			if len(cmd.Args) >= 4 {
				timeUnitStr := cmd.Args[2]
				timeUnitStr = strings.ToUpper(timeUnitStr)

				if !(timeUnitStr == "EX" || timeUnitStr == "PX") {
					conn.Write(parser.EncodeBulkString("ERR wrong expiry format"))
					continue
				}
				s, err := strconv.Atoi(cmd.Args[3])
				if err != nil {
					conn.Write(parser.EncodeBulkString("ERR wrong expiry format"))
					continue
				}
				if timeUnitStr == "EX" {
					// seconds
					expiry = time.Now().Add(time.Duration(s) * time.Second)
				} else {
					// milli seconds
					expiry = time.Now().Add(time.Duration(s) * time.Millisecond)
				}
			}
			app.store.Set(key, val, expiry)
			conn.Write(parser.EncodeString("OK"))
		case "GET":
			if len(cmd.Args) < 1 {
				conn.Write(parser.EncodeBulkString("ERR wrong number of arguments for 'get' command"))
				continue
			}
			val, ok := app.store.Get(cmd.Args[0])
			if !ok {
				conn.Write(parser.EncodeNullBulkString())
				continue
			}

			conn.Write(parser.EncodeBulkString(val))
		default:
			conn.Write(parser.EncodeString("ERR unknown command '" + cmd.Name + "'"))
		}
	}
}
