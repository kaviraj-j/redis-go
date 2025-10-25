package main

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

func (app *App) handlePing(req *Request) {
	req.Write(parser.EncodeString("PONG"))
}

func (app *App) handleInfo(req *Request) {
	includeReplication := slices.Index(req.cmd.Args, "replication") != -1
	if includeReplication {
		role := app.role
		content := fmt.Sprintf("role:%s\r\nmaster_repl_offset:%d\r\nmaster_replid:%s", role, app.replicationOffset, app.replicationId)
		req.Write(parser.EncodeBulkString(content))
		return
	}
	req.Write(parser.EncodeNullBulkString())
}

func (app *App) handleEcho(req *Request) {
	if len(req.cmd.Args) < 1 {
		req.Write(parser.EncodeWrongNumArgsError("echo"))
		return
	}
	req.Write(parser.EncodeBulkString(req.cmd.Args[0]))
}

func (app *App) handleReplconf(req *Request) {
	args := req.cmd.Args
	if len(args) == 2 && args[0] == "GETACK" && args[1] == "*" {
		req.conn.Write(parser.EncodeArray([]string{"REPLCONF", "ACK", strconv.Itoa(app.replicationOffset)}))
		return
	}
	req.conn.Write(parser.EncodeString("OK"))
}
func (app *App) handlePsync(req *Request) {
	if app.role != RoleMaster {
		req.Write(parser.EncodeError(fmt.Errorf("ERR not a master")))
		return
	}

	// register this slave
	replica := Replica{
		replicationId: app.replicationId,
		offset:        0,
		conn:          req.conn,
		addr:          req.conn.RemoteAddr(),
	}
	app.replicas = append(app.replicas, replica)
	fmt.Printf("New slave connected: %s\n", req.conn.RemoteAddr())

	req.Write(parser.EncodeString(fmt.Sprintf("FULLRESYNC %s %d", app.replicationId, app.replicationOffset)))

	data, err := os.ReadFile("./dump/empty.rdb")
	if err != nil {
		fmt.Println(err)
		return
	}
	req.Write([]byte(fmt.Sprintf("$%d\r\n", len(data))))
	req.Write(data)
}

func (app *App) handleWait(req *Request) {
	if app.role != RoleMaster {
		req.Write(parser.EncodeError(fmt.Errorf("ERR not a master")))
		return
	}
	if len(req.cmd.Args) < 2 {
		req.Write(parser.EncodeWrongNumArgsError("wait"))
		return
	}
	minReplicas, _ := strconv.Atoi(req.cmd.Args[0])
	waitUntil, _ := strconv.Atoi(req.cmd.Args[1])

	successSycn := make(chan struct{}, 1)
	total := 0

	if len(app.replicas) == 0 {
		req.conn.Write(parser.EncodeInt(0))
		return
	}

	if app.replicationOffset == 0 {
		req.conn.Write(parser.EncodeInt(len(app.replicas)))
		return
	}

	for _, replica := range app.replicas {
		go func() {
			replica.conn.Write(parser.EncodeArray([]string{"REPLCONF", "GETACK", "*"}))
			data := make([]byte, 4)
			_, err := replica.conn.Read(data)
			if err != nil {
				fmt.Println("Error in reading response", err)
				return
			}
			fmt.Println(string(data))
			successSycn <- struct{}{}
		}()
	}

	for {
		select {
		case <-time.After(time.Duration(waitUntil) * time.Millisecond):
			req.Write(parser.EncodeInt(total))
			return
		case <-successSycn:
			total++
			if total >= minReplicas {
				req.Write(parser.EncodeInt(total))
				return
			}
		}
	}

}

func (app *App) handleSet(req *Request) {
	if len(req.cmd.Args) < 2 {
		req.Write(parser.EncodeWrongNumArgsError("set"))
		return
	}

	key, val := req.cmd.Args[0], req.cmd.Args[1]
	expiry, err := parseExpiry(req.cmd)
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}

	app.store.SetString(key, val, expiry)

	if app.role == RoleMaster && !req.isFromMaster {
		app.broadcastToReplicas(req.cmd)
	}

	req.Write(parser.EncodeString("OK"))
}

func (app *App) handleGet(req *Request) {
	if len(req.cmd.Args) < 1 {
		req.Write(parser.EncodeWrongNumArgsError("get"))
		return
	}

	valStr, ok, err := app.store.Get(req.cmd.Args[0])
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}

	if !ok {
		req.Write(parser.EncodeNullBulkString())
		return
	}

	req.Write(parser.EncodeBulkString(valStr))
}

func (app *App) handleIncrement(req *Request) {
	if len(req.cmd.Args) < 1 {
		req.Write(parser.EncodeWrongNumArgsError("incr"))
		return
	}

	n, err := app.store.Increment(req.cmd.Args[0])
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}

	if app.role == RoleMaster && !req.isFromMaster {
		app.broadcastToReplicas(req.cmd)
	}

	req.Write([]byte(fmt.Sprintf(":%d\r\n", n)))
}

func (app *App) handlePush(req *Request) {
	if len(req.cmd.Args) < 2 {
		req.Write(parser.EncodeWrongNumArgsError(strings.ToLower(req.cmd.Name)))
		return
	}

	key := req.cmd.Args[0]
	values := req.cmd.Args[1:]
	n, err := app.store.PushList(key, values, store.PushListDirection(req.cmd.Name))
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}

	if app.role == RoleMaster && !req.isFromMaster {
		app.broadcastToReplicas(req.cmd)
	}

	req.Write(parser.EncodeInt(n))
}

func (app *App) handleLRange(req *Request) {
	if len(req.cmd.Args) < 3 {
		req.Write(parser.EncodeWrongNumArgsError("lrange"))
		return
	}

	start, err := strconv.Atoi(req.cmd.Args[1])
	if err != nil {
		req.Write(parser.EncodeInvalidIntError("start"))
		return
	}

	end, err := strconv.Atoi(req.cmd.Args[2])
	if err != nil {
		req.Write(parser.EncodeInvalidIntError("end"))
		return
	}

	values, err := app.store.LRange(req.cmd.Args[0], start, end)
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}

	req.Write(parser.EncodeArray(values))
}

func (app *App) handleLLen(req *Request) {
	if len(req.cmd.Args) < 1 {
		req.Write(parser.EncodeWrongNumArgsError("llen"))
		return
	}

	key := req.cmd.Args[0]
	n, err := app.store.GetListLen(key)
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}

	req.Write(parser.EncodeInt(n))
}

func (app *App) handleLPop(req *Request) {
	if len(req.cmd.Args) < 1 {
		req.Write(parser.EncodeWrongNumArgsError("lpop"))
		return
	}

	key, count, rmMultiple := req.cmd.Args[0], 1, false
	if len(req.cmd.Args) >= 2 {
		var err error
		count, err = strconv.Atoi(req.cmd.Args[1])
		if err != nil {
			req.Write(parser.EncodeInvalidIntError("count"))
			return
		}
		rmMultiple = true
	}

	res, err := app.store.ListPop(key, count)
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}

	if app.role == RoleMaster && !req.isFromMaster {
		app.broadcastToReplicas(req.cmd)
	}

	if !rmMultiple {
		if len(res) == 0 {
			req.Write(parser.EncodeNullBulkString())
		} else {
			req.Write(parser.EncodeBulkString(res[0]))
		}
		return
	}

	req.Write(parser.EncodeArray(res))
}

func (app *App) handleBLPop(req *Request) {
	if len(req.cmd.Args) < 2 {
		req.Write(parser.EncodeWrongNumArgsError("blpop"))
		return
	}

	key := req.cmd.Args[0]
	timeoutStr := req.cmd.Args[1]

	timeoutSec, err := strconv.ParseFloat(timeoutStr, 64)
	if err != nil {
		req.Write(parser.EncodeError(fmt.Errorf("ERR timeout is not a float")))
		return
	}

	doneChan, err := app.store.BlockListPop(key)
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}
	// Infinite block if timeout = 0
	if timeoutSec == 0 {
		val := <-doneChan
		response := []string{key, val}
		req.Write(parser.EncodeArray(response))
		return
	}

	select {
	case val := <-doneChan:
		// Success
		response := []string{key, val}
		req.Write(parser.EncodeArray(response))
	case <-time.After(time.Duration(timeoutSec * float64(time.Second))):
		// Timeout
		app.store.RemoveBlockedListChannel(key, doneChan)
		req.Write(parser.EncodeNullArray())
	}
}

func (app *App) handleXAdd(req *Request) {
	if len(req.cmd.Args) < 2 {
		req.Write(parser.EncodeWrongNumArgsError("xadd"))
		return
	}
	fields := make(map[string]string)
	fieldData := req.cmd.Args[2:]
	if len(fieldData)%2 != 0 {
		req.Write(parser.EncodeWrongNumArgsError("xadd"))
		return
	}
	for i := 0; i < len(fieldData); i += 2 {
		k, v := fieldData[i], fieldData[i+1]
		fields[k] = v
	}
	id, err := app.store.XAdd(req.cmd.Args[0], req.cmd.Args[1], fields)
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}

	if app.role == RoleMaster && !req.isFromMaster {
		app.broadcastToReplicas(req.cmd)
	}

	req.Write(parser.EncodeBulkString(id))
}

func (app *App) handleXRange(req *Request) {
	if len(req.cmd.Args) < 3 {
		req.Write(parser.EncodeWrongNumArgsError("xrange"))
		return
	}

	res, err := app.store.XRange(req.cmd.Args[0], req.cmd.Args[1], req.cmd.Args[2])
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}

	req.Write([]byte(fmt.Sprintf("*%d\r\n", len(res))))
	for _, entry := range res {
		req.Write([]byte("*2\r\n"))
		// write the id
		req.Write(parser.EncodeBulkString(entry[0]))
		// write the key values pairs
		req.Write(parser.EncodeArray(entry[1:]))
	}
}

func (app *App) handleXRead(req *Request) {
	if len(req.cmd.Args) < 3 {
		req.Write(parser.EncodeWrongNumArgsError("xread"))
		return
	}
	if strings.ToUpper(req.cmd.Args[0]) == "BLOCK" {
		app.handleXReadBlocked(req)
		return
	}
	if strings.ToUpper(req.cmd.Args[0]) != "STREAMS" {
		req.Write(parser.EncodeError(fmt.Errorf("ERR first arg must be 'STREAMS' or 'BLOCK'")))
		return
	}
	keysAndIds := req.cmd.Args[1:]
	if len(keysAndIds)%2 != 0 {
		req.Write(parser.EncodeError(fmt.Errorf("ERR equal number of key and id must be there")))
		return
	}

	halfLen := len(keysAndIds) / 2
	keys := keysAndIds[:halfLen]
	ids := keysAndIds[halfLen:]

	res, err := app.store.XRead(keys, ids)
	if err != nil {
		req.Write(parser.EncodeError(err))
		return
	}
	writeStreamReadData(req, res)
}
func (app *App) handleXReadBlocked(req *Request) {
	waitUntilMs, _ := strconv.Atoi(req.cmd.Args[1])
	if strings.ToUpper(req.cmd.Args[2]) != "STREAMS" {
		req.Write(parser.EncodeError(fmt.Errorf("ERR missing arg 'STREAMS'")))
		return
	}

	keysAndIds := req.cmd.Args[3:]
	waitForNewEntry := false
	if keysAndIds[len(keysAndIds)-1] == "$" {
		waitForNewEntry = true
	}

	if !waitForNewEntry && len(keysAndIds)%2 != 0 {
		req.Write(parser.EncodeError(fmt.Errorf("ERR equal number of key and id must be there")))
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
		req.Write(parser.EncodeError(err))
		return
	}

	if waitUntilMs == 0 {
		// wait indefinitely
		response := <-doneChan
		writeStreamReadData(req, response)
		return
	}

	select {
	case response := <-doneChan:
		for _, key := range keys {
			app.store.RemoveBlockedStreamChannel(key, doneChan)
		}
		writeStreamReadData(req, response)
	case <-time.After(time.Duration(waitUntilMs) * time.Millisecond):
		req.Write(parser.EncodeNullArray())
		go func() {
			for _, key := range keys {
				app.store.RemoveBlockedStreamChannel(key, doneChan)
			}
		}()
	}
}

func (app *App) handleGetType(req *Request) {
	if len(req.cmd.Args) < 1 {
		req.Write(parser.EncodeWrongNumArgsError("type"))
		return
	}

	t := app.store.GetType(req.cmd.Args[0])

	req.Write(parser.EncodeString(t))
}
