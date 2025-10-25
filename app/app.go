package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
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
		// Create a reader that will be shared between handshake and ongoing communication
		reader := bufio.NewReader(app.masterServer.conn)

		fmt.Println("Slave: Starting handshake with master...")
		err := app.handshakeWithMaster(reader)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Slave: Handshake completed successfully")

		// Use the same reader for handling subsequent commands
		go app.handleMasterConnection(app.masterServer.conn, reader)
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

func (app *App) handleMasterConnection(conn net.Conn, reader *bufio.Reader) {
	fmt.Println("Handling master connection from", conn.RemoteAddr())
	defer conn.Close()

	isQueued := false
	cmdQueue := make([]*parser.Command, 0)

	for {
		cmd, consumed, err := parser.ParseRequest(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Master disconnected:", conn.RemoteAddr())
				return
			}
			fmt.Println("Error parsing request from master:", err)
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
		app.replicationOffset += consumed
	}
}

func (app *App) handleConnection(conn net.Conn) {
	fmt.Println("Handling new connection from", conn.RemoteAddr())
	defer conn.Close()

	reader := bufio.NewReader(conn)

	isQueued := false
	cmdQueue := make([]*parser.Command, 0)

	for {
		cmd, _, err := parser.ParseRequest(reader)
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
	case "WAIT":
		app.handleWait(req)
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
