package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/parser"
)

func createConnectionWithMaster(replicaOf string) (MasterServer, error) {
	parts := strings.Split(replicaOf, " ")
	if len(parts) != 2 {
		return MasterServer{}, fmt.Errorf("invalid master server details")
	}

	host, port := parts[0], parts[1]
	masterServerAddr := net.JoinHostPort(host, port)
	conn, err := net.Dial("tcp", masterServerAddr)
	if err != nil {
		return MasterServer{}, err
	}
	return MasterServer{
		addr: masterServerAddr,
		conn: conn,
	}, nil
}

func (app *App) handshakeWithMaster() error {
	tcpAddr := app.listener.Addr().(*net.TCPAddr)
	port := tcpAddr.Port

	conn := app.masterServer.conn
	tmpData := make([]byte, 1024)
	// PING
	conn.Write(parser.EncodeArray([]string{"PING"}))
	conn.Read(tmpData)

	// REPLCONF listening-port
	conn.Write(parser.EncodeArray([]string{"REPLCONF", "listening-port", strconv.Itoa(port)}))
	conn.Read(tmpData)

	// REPLCONF capa
	conn.Write(parser.EncodeArray([]string{"REPLCONF", "capa", "psync2"}))
	conn.Read(tmpData)

	// PSYNC
	conn.Write(parser.EncodeArray([]string{"PSYNC", "?", "-1"}))
	conn.Read(tmpData)

	// rdb file
	conn.Read(tmpData)

	return nil
}
