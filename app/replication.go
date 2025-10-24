package main

import (
	"bufio"
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
	fmt.Println("port:", port)

	conn := app.masterServer.conn
	tmpReader := bufio.NewReader(conn)
	tmpData := make([]byte, 1024)
	conn.Write(parser.EncodeArray([]string{"PING"}))
	tmpReader.Read(tmpData)
	conn.Write(parser.EncodeArray([]string{"REPLCONF", "listening-port", strconv.Itoa(port)}))
	tmpReader.Read(tmpData)
	conn.Write(parser.EncodeArray([]string{"REPLCONF", "capa", "psync2"}))
	tmpReader.Read(tmpData)
	conn.Write(parser.EncodeArray([]string{"PSYNC", "?", "-1"}))
	tmpReader.Read(tmpData)
	return nil
}
