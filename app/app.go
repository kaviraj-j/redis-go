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
			expiry, err := getExpiry(cmd)
			if err != nil {
				conn.Write(parser.EncodeBulkString(err.Error()))
				continue
			}
			app.store.SetString(key, val, expiry)
			conn.Write(parser.EncodeString("OK"))
		case "GET":
			// get string
			if len(cmd.Args) < 1 {
				conn.Write(parser.EncodeBulkString("ERR wrong number of arguments for 'get' command"))
				continue
			}
			valStr, ok, err := app.store.Get(cmd.Args[0])
			if err != nil {
				conn.Write(parser.EncodeString(err.Error()))
				continue
			}
			if !ok {
				conn.Write(parser.EncodeNullBulkString())
				continue
			}

			conn.Write(parser.EncodeBulkString(valStr))
		case "RPUSH", "LPUSH":
			if len(cmd.Args) < 2 {
				conn.Write(parser.EncodeBulkString("ERR wrong number of arguments for 'RPUSH' command"))
				continue
			}
			key := cmd.Args[0]
			values := cmd.Args[1:]
			n, err := app.store.PushList(key, values, store.PushListDirection(cmd.Name))
			if err != nil {
				conn.Write(parser.EncodeBulkString(err.Error()))
				continue
			}
			conn.Write(parser.EncodeInt(n))
		case "LRANGE":
			if len(cmd.Args) < 3 {
				conn.Write(parser.EncodeBulkString("ERR wrong number of arguments for 'LRANGE' command"))
				continue
			}
			start, err := strconv.Atoi(cmd.Args[1])
			if err != nil {
				conn.Write(parser.EncodeBulkString("ERR invalid index"))
			}
			end, err := strconv.Atoi(cmd.Args[2])
			if err != nil {
				conn.Write(parser.EncodeBulkString("ERR invalid index"))
			}
			values := app.store.LRange(cmd.Args[0], start, end)
			conn.Write(parser.EncodeArray(values))
		case "LLEN":
			if len(cmd.Args) < 1 {
				conn.Write(parser.EncodeBulkString("ERR wrong number of arguments for 'LLEN' command"))
				continue
			}
			key := cmd.Args[0]
			n := app.store.GetListLen(key)
			conn.Write(parser.EncodeInt(n))
		case "LPOP":
			if len(cmd.Args) < 1 {
				conn.Write(parser.EncodeBulkString("ERR wrong number of arguments for 'LPOP' command"))
				continue
			}
			key, count := cmd.Args[0], 1
			if len(cmd.Args) >= 2 {
				countStr := cmd.Args[1]
				count, err = strconv.Atoi(countStr)
				if err != nil {
					conn.Write(parser.EncodeBulkString("ERR invalid count"))
				}
			}
			res := app.store.ListPop(key, count)
			if len(res) == 0 {
				conn.Write(parser.EncodeNullBulkString())
			} else {
				conn.Write(parser.EncodeArray(res))
			}
		default:
			conn.Write(parser.EncodeString("ERR unknown command '" + cmd.Name + "'"))
		}
	}
}

func getExpiry(cmd *parser.Command) (time.Time, error) {
	var expiry time.Time
	if len(cmd.Args) >= 4 {
		timeUnitStr := cmd.Args[2]
		timeUnitStr = strings.ToUpper(timeUnitStr)

		if !(timeUnitStr == "EX" || timeUnitStr == "PX") {
			return expiry, fmt.Errorf("ERR wrong expiry format")
		}
		s, err := strconv.Atoi(cmd.Args[3])
		if err != nil {
			return expiry, fmt.Errorf("ERR wrong expiry format")
		}
		if timeUnitStr == "EX" {
			// seconds
			expiry = time.Now().Add(time.Duration(s) * time.Second)
		} else {
			// milli seconds
			expiry = time.Now().Add(time.Duration(s) * time.Millisecond)
		}
	}
	return expiry, nil
}
