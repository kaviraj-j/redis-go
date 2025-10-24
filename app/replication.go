package main

import (
	"fmt"
	"net"
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
	// addr := app.listner.Addr()
	// port := strings.Split(addr.String(), ":")[1]
	conn := app.masterServer.conn
	conn.Write(parser.EncodeArray([]string{"PING"}))
	// conn.Write(parser.EncodeArray([]string{"REPLCONF", "listening-port ", port}))
	// conn.Write(parser.EncodeArray([]string{"REPLCONF", "capa", "psync2"}))
	// conn.Write(parser.EncodeArray([]string{"PSYNC", app.replicaDetails.replicationId, strconv.Itoa(app.replicaDetails.offest)}))
	return nil
}
