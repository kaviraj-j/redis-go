package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

type ServerRole string

const (
	RoleMaster ServerRole = "master"
	RoleSlave  ServerRole = "slave"
)

type MasterServer struct {
	addr string
	conn net.Conn
}

type Replica struct {
	replicationId string
	offset        int
	conn          net.Conn
	addr          net.Addr
}

type Request struct {
	conn         net.Conn
	cmd          *parser.Command
	isFromMaster bool
}

func (req *Request) Write(data []byte) (int, error) {
	if req.isFromMaster {
		return len(data), nil
	}
	return req.conn.Write(data)
}

type App struct {
	listener          net.Listener
	store             *store.Store
	role              ServerRole
	masterServer      MasterServer
	replicas          []Replica
	replicationId     string
	replicationOffset int
}

func newApp(listener net.Listener, role ServerRole, masterServer MasterServer) *App {
	s, _ := store.NewStore()
	return &App{
		listener:          listener,
		store:             s,
		role:              role,
		masterServer:      masterServer,
		replicationId:     generateReplicationId(),
		replicationOffset: 0,
	}
}

func (app *App) run() {
	if app.role == RoleSlave {
		// send hand shake to master
		fmt.Println("Slave: Starting handshake with master...")
		err := app.handshakeWithMaster()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Slave: Handshake completed successfully")
		go app.handleConnection(app.masterServer.conn)
	}
	for {
		conn, err := app.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		fmt.Printf("New connection accepted from: %s\n", conn.RemoteAddr().String())
		go app.handleConnection(conn)
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
	isFromMaster := false
	if app.role == RoleSlave {
		if app.masterServer.conn != nil {
			isFromMaster = (app.masterServer.conn.RemoteAddr().String() == conn.RemoteAddr().String())
		}
		fmt.Printf("SLAVE: Executing command '%s' from %s, isFromMaster=%v\n", cmd.Name, conn.RemoteAddr().String(), isFromMaster)
		if app.masterServer.conn != nil {
			fmt.Printf("SLAVE: Master conn addr: %s\n", app.masterServer.conn.RemoteAddr().String())
		}
	} else if app.role == RoleMaster {
		for _, replica := range app.replicas {
			if replica.conn.RemoteAddr().String() == conn.RemoteAddr().String() {
				isFromMaster = false
				break
			}
		}
		fmt.Printf("MASTER: Executing command '%s' from %s, isFromMaster=%v\n", cmd.Name, conn.RemoteAddr().String(), isFromMaster)
	}

	req := &Request{
		conn:         conn,
		cmd:          cmd,
		isFromMaster: isFromMaster,
	}

	switch strings.ToUpper(cmd.Name) {
	case "PING":
		app.handlePing(req)
	case "INFO":
		app.handleInfo(req)
	case "ECHO":
		app.handleEcho(req)
	case "REPLCONF":
		app.handleReplconf(req)
	case "PSYNC":
		app.handlePsync(req)
	case "SET":
		app.handleSet(req)
	case "GET":
		app.handleGet(req)
	case "INCR":
		app.handleIncrement(req)
	case "RPUSH", "LPUSH":
		app.handlePush(req)
	case "LRANGE":
		app.handleLRange(req)
	case "LLEN":
		app.handleLLen(req)
	case "LPOP":
		app.handleLPop(req)
	case "BLPOP":
		app.handleBLPop(req)
	case "TYPE":
		app.handleGetType(req)
	case "XADD":
		app.handleXAdd(req)
	case "XRANGE":
		app.handleXRange(req)
	case "XREAD":
		app.handleXRead(req)
	default:
		req.Write(parser.EncodeError(fmt.Errorf("ERR unknown command '%s'", cmd.Name)))
	}
}

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
func writeStreamReadData(req *Request, response []store.XReadResponse) {
	req.Write([]byte(fmt.Sprintf("*%d\r\n", len(response))))
	for _, r := range response {
		req.Write([]byte("*2\r\n"))
		req.Write(parser.EncodeBulkString(r.Key))
		req.Write([]byte(fmt.Sprintf("*%d\r\n", len(r.Entries))))
		for _, entry := range r.Entries {
			req.Write([]byte("*2\r\n"))
			req.Write(parser.EncodeBulkString(entry.Id))
			req.Write(parser.EncodeArray(entry.KeyValue))
		}
	}
}

func (app *App) broadcastToReplicas(cmd *parser.Command) {
	fmt.Printf("MASTER: Broadcasting command '%s' to %d replicas\n", cmd.Name, len(app.replicas))
	if len(app.replicas) == 0 {
		fmt.Println("MASTER: No replicas to broadcast to")
		return
	}

	var failedReplicas []int
	for i, replica := range app.replicas {
		cmdBytes := parser.EncodeArray(append([]string{cmd.Name}, cmd.Args...))
		fmt.Printf("MASTER: Sending command to replica %s: %s\n", replica.addr, cmd.Name)
		_, err := replica.conn.Write(cmdBytes)
		if err != nil {
			failedReplicas = append(failedReplicas, i)
			fmt.Printf("MASTER: Failed to replicate to slave %s: %v\n", replica.addr, err)
		} else {
			fmt.Printf("MASTER: Successfully sent command to replica %s\n", replica.addr)
		}
	}

	for i := len(failedReplicas) - 1; i >= 0; i-- {
		idx := failedReplicas[i]
		app.replicas = append(app.replicas[:idx], app.replicas[idx+1:]...)
	}
}
