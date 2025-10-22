package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

type App struct {
	store  *store.Store
	config Config
}

type Config struct {
	replicaOf string
}

func newApp(config Config) *App {
	s, _ := store.NewStore()
	return &App{
		store:  s,
		config: config,
	}
}

func (app *App) handleConnection(conn net.Conn) {
	fmt.Println("Handling new connection from", conn.RemoteAddr())
	defer conn.Close()

	reader := bufio.NewReader(conn)

	isQueued := false
	cmdQueue := make([]*parser.Command, 0)

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

		cmdUpper := strings.ToUpper(cmd.Name)

		// Handle MULTI command
		if cmdUpper == "MULTI" {
			if isQueued {
				conn.Write(parser.EncodeError(fmt.Errorf("ERR cmds are queued already")))
				continue
			}
			isQueued = true
			cmdQueue = make([]*parser.Command, 0)
			conn.Write(parser.EncodeString("OK"))
			continue
		} else if cmdUpper == "EXEC" {
			if !isQueued {
				conn.Write(parser.EncodeError(fmt.Errorf("ERR EXEC without MULTI")))
				continue
			}

			conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(cmdQueue))))
			for _, queuedCmd := range cmdQueue {
				app.executeCmd(conn, queuedCmd)
			}

			isQueued = false
			cmdQueue = make([]*parser.Command, 0)
			continue
		} else if cmdUpper == "DISCARD" {
			if !isQueued {
				conn.Write(parser.EncodeError(fmt.Errorf("ERR DISCARD without MULTI")))
				continue
			}
			isQueued = false
			cmdQueue = make([]*parser.Command, 0)
			conn.Write(parser.EncodeString("OK"))
			continue
		}

		if isQueued {
			cmdQueue = append(cmdQueue, cmd)
			conn.Write(parser.EncodeString("QUEUED"))
		} else {
			app.executeCmd(conn, cmd)
		}
	}
}

func (app *App) executeCmd(conn net.Conn, cmd *parser.Command) {
	switch strings.ToUpper(cmd.Name) {
	case "PING":
		app.handlePing(conn)
	case "INFO":
		app.handleInfo(conn, cmd)
	case "ECHO":
		app.handleEcho(conn, cmd)
	case "SET":
		app.handleSet(conn, cmd)
	case "GET":
		app.handleGet(conn, cmd)
	case "INCR":
		app.handleIncrement(conn, cmd)
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
	case "TYPE":
		app.handleGetType(conn, cmd)
	case "XADD":
		app.handleXAdd(conn, cmd)
	case "XRANGE":
		app.handleXRange(conn, cmd)
	case "XREAD":
		app.handleXRead(conn, cmd)
	default:
		conn.Write(parser.EncodeError(fmt.Errorf("ERR unknown command '%s'", cmd.Name)))
	}
}

func (app *App) handlePing(conn net.Conn) {
	conn.Write(parser.EncodeString("PONG"))
}

func (app *App) handleInfo(conn net.Conn, cmd *parser.Command) {
	includeReplication := slices.Index(cmd.Args, "replication") != -1
	if includeReplication {
		role := "master"
		if len(app.config.replicaOf) > 0 {
			role = "slave"
		}
		conn.Write(parser.EncodeBulkString("role:" + role))
	}
	conn.Write(parser.EncodeNullBulkString())
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

func (app *App) handleIncrement(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 1 {
		conn.Write(parser.EncodeWrongNumArgsError("incr"))
		return
	}

	n, err := app.store.Increment(cmd.Args[0])
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))
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
		app.store.RemoveBlockedListChannel(key, doneChan)
		conn.Write(parser.EncodeNullArray())
	}
}

func (app *App) handleXAdd(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 2 {
		conn.Write(parser.EncodeWrongNumArgsError("xadd"))
		return
	}
	fields := make(map[string]string)
	fieldData := cmd.Args[2:]
	if len(fieldData)%2 != 0 {
		conn.Write(parser.EncodeWrongNumArgsError("xadd"))
		return
	}
	for i := 0; i < len(fieldData); i += 2 {
		k, v := fieldData[i], fieldData[i+1]
		fields[k] = v
	}
	id, err := app.store.XAdd(cmd.Args[0], cmd.Args[1], fields)
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}
	conn.Write(parser.EncodeBulkString(id))
}

func (app *App) handleXRange(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 3 {
		conn.Write(parser.EncodeWrongNumArgsError("xrange"))
		return
	}

	res, err := app.store.XRange(cmd.Args[0], cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}

	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(res))))
	for _, entry := range res {
		conn.Write([]byte("*2\r\n"))
		// write the id
		conn.Write(parser.EncodeBulkString(entry[0]))
		// write the key values pairs
		conn.Write(parser.EncodeArray(entry[1:]))
	}
}

func (app *App) handleXRead(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 3 {
		conn.Write(parser.EncodeWrongNumArgsError("xread"))
		return
	}
	if strings.ToUpper(cmd.Args[0]) == "BLOCK" {
		app.handleXReadBlocked(conn, cmd)
		return
	}
	if strings.ToUpper(cmd.Args[0]) != "STREAMS" {
		conn.Write(parser.EncodeError(fmt.Errorf("ERR first arg must be 'STREAMS' or 'BLOCK'")))
		return
	}
	keysAndIds := cmd.Args[1:]
	if len(keysAndIds)%2 != 0 {
		conn.Write(parser.EncodeError(fmt.Errorf("ERR equal number of key and id must be there")))
		return
	}

	halfLen := len(keysAndIds) / 2
	keys := keysAndIds[:halfLen]
	ids := keysAndIds[halfLen:]

	res, err := app.store.XRead(keys, ids)
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}
	writeStreamReadData(conn, res)
}
func (app *App) handleXReadBlocked(conn net.Conn, cmd *parser.Command) {
	waitUntilMs, _ := strconv.Atoi(cmd.Args[1])
	if strings.ToUpper(cmd.Args[2]) != "STREAMS" {
		conn.Write(parser.EncodeError(fmt.Errorf("ERR missing arg 'STREAMS'")))
		return
	}

	keysAndIds := cmd.Args[3:]
	waitForNewEntry := false
	if keysAndIds[len(keysAndIds)-1] == "$" {
		waitForNewEntry = true
	}

	if !waitForNewEntry && len(keysAndIds)%2 != 0 {
		conn.Write(parser.EncodeError(fmt.Errorf("ERR equal number of key and id must be there")))
		return
	}

	// Preserve key order in a slice
	var keys []string
	var ids []string
	if !waitForNewEntry {
		halfLen := len(keysAndIds) / 2
		keys = keysAndIds[:halfLen]
		ids = keysAndIds[halfLen:]

	} else {
		keys = keysAndIds[:len(keysAndIds)-1]
		ids = make([]string, len(keys))
	}

	doneChan, err := app.store.XReadBlocked(keys, ids, waitForNewEntry)
	if err != nil {
		conn.Write(parser.EncodeError(err))
		return
	}

	if waitUntilMs == 0 {
		// wait indefinitely
		response := <-doneChan
		writeStreamReadData(conn, response)
		return
	}

	select {
	case response := <-doneChan:
		for _, key := range keys {
			app.store.RemoveBlockedStreamChannel(key, doneChan)
		}
		writeStreamReadData(conn, response)
	case <-time.After(time.Duration(waitUntilMs) * time.Millisecond):
		conn.Write(parser.EncodeNullArray())
		go func() {
			for _, key := range keys {
				app.store.RemoveBlockedStreamChannel(key, doneChan)
			}
		}()
	}
}

func (app *App) handleGetType(conn net.Conn, cmd *parser.Command) {
	if len(cmd.Args) < 1 {
		conn.Write(parser.EncodeWrongNumArgsError("type"))
		return
	}

	t := app.store.GetType(cmd.Args[0])

	conn.Write(parser.EncodeString(t))
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

// ---- helpers ----
func writeStreamReadData(conn net.Conn, response []store.XReadResponse) {
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(response))))
	for _, r := range response {
		conn.Write([]byte("*2\r\n"))
		conn.Write(parser.EncodeBulkString(r.Key))
		conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(r.Entries))))
		for _, entry := range r.Entries {
			conn.Write([]byte("*2\r\n"))
			conn.Write(parser.EncodeBulkString(entry.Id))
			conn.Write(parser.EncodeArray(entry.KeyValue))
		}
	}
}
