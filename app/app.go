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
			app.handlePing(conn)
		case "ECHO":
			app.handleEcho(conn, cmd)
		case "SET":
			app.handleSet(conn, cmd)
		case "GET":
			app.handleGet(conn, cmd)
		case "RPUSH", "LPUSH":
			app.handlePush(conn, cmd)
		case "LRANGE":
			app.handleLRange(conn, cmd)
		case "LLEN":
			app.handleLLen(conn, cmd)
		case "LPOP":
			app.handleLPop(conn, cmd)
		case "BLPOP":
			app.handleBLPop(conn, cmd)
		default:
			conn.Write(parser.EncodeError(fmt.Errorf("ERR unknown command '%s'", cmd.Name)))
		}
	}
}

func (app *App) handlePing(conn net.Conn) {
	conn.Write(parser.EncodeString("PONG"))
}

func (app *App) handleEcho(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 1 {
		conn.Write(parser.EncodeWrongNumArgsError("echo"))
		return
	}
	conn.Write(parser.EncodeBulkString(cmd.Args[0]))
}

func (app *App) handleSet(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 2 {
		conn.Write(parser.EncodeWrongNumArgsError("set"))
		return
	}

	key, val := cmd.Args[0], cmd.Args[1]
	expiry, err := parseExpiry(cmd)
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}

	app.store.SetString(key, val, expiry)
	conn.Write(parser.EncodeString("OK"))
}

func (app *App) handleGet(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 1 {
		conn.Write(parser.EncodeWrongNumArgsError("get"))
		return
	}

	valStr, ok, err := app.store.Get(cmd.Args[0])
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}

	if !ok {
		conn.Write(parser.EncodeNullBulkString())
		return
	}

	conn.Write(parser.EncodeBulkString(valStr))
}

func (app *App) handlePush(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 2 {
		conn.Write(parser.EncodeWrongNumArgsError(strings.ToLower(cmd.Name)))
		return
	}

	key := cmd.Args[0]
	values := cmd.Args[1:]
	n, err := app.store.PushList(key, values, store.PushListDirection(cmd.Name))
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}

	conn.Write(parser.EncodeInt(n))
}

func (app *App) handleLRange(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 3 {
		conn.Write(parser.EncodeWrongNumArgsError("lrange"))
		return
	}

	start, err := strconv.Atoi(cmd.Args[1])
	if err != nil {
		conn.Write(parser.EncodeInvalidIntError("start"))
		return
	}

	end, err := strconv.Atoi(cmd.Args[2])
	if err != nil {
		conn.Write(parser.EncodeInvalidIntError("end"))
		return
	}

	values, err := app.store.LRange(cmd.Args[0], start, end)
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}

	conn.Write(parser.EncodeArray(values))
}

func (app *App) handleLLen(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 1 {
		conn.Write(parser.EncodeWrongNumArgsError("llen"))
		return
	}

	key := cmd.Args[0]
	n, err := app.store.GetListLen(key)
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}

	conn.Write(parser.EncodeInt(n))
}

func (app *App) handleLPop(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 1 {
		conn.Write(parser.EncodeWrongNumArgsError("lpop"))
		return
	}

	key, count, rmMultiple := cmd.Args[0], 1, false
	if len(cmd.Args) >= 2 {
		var err error
		count, err = strconv.Atoi(cmd.Args[1])
		if err != nil {
			conn.Write(parser.EncodeInvalidIntError("count"))
			return
		}
		rmMultiple = true
	}

	res, err := app.store.ListPop(key, count)
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}

	if !rmMultiple {
		if len(res) == 0 {
			conn.Write(parser.EncodeNullBulkString())
		} else {
			conn.Write(parser.EncodeBulkString(res[0]))
		}
		return
	}

	conn.Write(parser.EncodeArray(res))
}

func (app *App) handleBLPop(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 2 {
		conn.Write(parser.EncodeWrongNumArgsError("blpop"))
		return
	}

	key := cmd.Args[0]
	timeoutStr := cmd.Args[1]

	timeoutSec, err := strconv.ParseFloat(timeoutStr, 64)
	fmt.Println("timeoutSec", timeoutSec)
	if err != nil {
		conn.Write(parser.EncodeError(fmt.Errorf("ERR timeout is not a float")))
		return
	}

	doneChan, err := app.store.BlockListPop(key)
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}
	// Infinite block if timeout = 0
	if timeoutSec == 0 {
		val := <-doneChan
		response := []string{key, val}
		conn.Write(parser.EncodeArray(response))
		return
	}

	select {
	case val := <-doneChan:
		// Success
		response := []string{key, val}
		conn.Write(parser.EncodeArray(response))
	case <-time.After(time.Duration(timeoutSec * float64(time.Second))):
		// Timeout
		app.store.RemoveBlockedChannel(key, doneChan)
		conn.Write(parser.EncodeNullArray())
	}
}

func parseExpiry(cmd *parser.Command) (time.Time, error) {
	var expiry time.Time
	if len(cmd.Args) < 4 {
		return expiry, nil
	}

	timeUnitStr := strings.ToUpper(cmd.Args[2])
	if timeUnitStr != "EX" && timeUnitStr != "PX" {
		return expiry, fmt.Errorf("ERR syntax error")
	}

	duration, err := strconv.Atoi(cmd.Args[3])
	if err != nil {
		return expiry, fmt.Errorf("ERR value is not an integer or out of range")
	}

	if timeUnitStr == "EX" {
		expiry = time.Now().Add(time.Duration(duration) * time.Second)
	} else {
		expiry = time.Now().Add(time.Duration(duration) * time.Millisecond)
	}

	return expiry, nil
}
