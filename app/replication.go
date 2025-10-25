package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
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

func (app *App) handshakeWithMaster(reader *bufio.Reader) error {
	tcpAddr := app.listener.Addr().(*net.TCPAddr)
	port := tcpAddr.Port

	conn := app.masterServer.conn

	readResponse := func(step string) error {
		resp, err := readRESPReply(reader)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("[%s] Read error: %w", step, err)
		}
		fmt.Printf("[%s] Response: %s\n", step, resp)
		return nil
	}

	fmt.Println("=> Sending PING")
	conn.Write(parser.EncodeArray([]string{"PING"}))
	if err := readResponse("PING"); err != nil {
		return err
	}

	// REPLCONF listening-port
	fmt.Println("=> Sending REPLCONF listening-port")
	conn.Write(parser.EncodeArray([]string{"REPLCONF", "listening-port", strconv.Itoa(port)}))
	if err := readResponse("REPLCONF listening-port"); err != nil {
		return err
	}

	fmt.Println("=> Sending REPLCONF capa")
	conn.Write(parser.EncodeArray([]string{"REPLCONF", "capa", "psync2"}))
	if err := readResponse("REPLCONF capa"); err != nil {
		return err
	}

	fmt.Println("=> Sending PSYNC")
	conn.Write(parser.EncodeArray([]string{"PSYNC", "?", "-1"}))
	if err := readResponse("PSYNC"); err != nil {
		return err
	}

	// Read RDB file
	fmt.Println("=> Reading RDB file data")
	// Read the bulk string length marker
	prefix, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read RDB prefix: %w", err)
	}
	if prefix != '$' {
		return fmt.Errorf("expected '$' for RDB, got %c", prefix)
	}

	// Read the length
	lenLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read RDB length: %w", err)
	}
	length, err := strconv.Atoi(strings.TrimSpace(lenLine))
	if err != nil {
		return fmt.Errorf("invalid RDB length: %w", err)
	}

	rdbData := make([]byte, length)
	if _, err := io.ReadFull(reader, rdbData); err != nil {
		return fmt.Errorf("failed to read RDB data: %w", err)
	}
	fmt.Printf("Successfully read %d bytes of RDB data\n", length)

	return nil
}

func readRESPReply(r *bufio.Reader) (string, error) {
	prefix, err := r.Peek(1)
	if err != nil {
		return "", err
	}

	switch prefix[0] {
	case '+', '-', ':':
		line, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(line), nil

	case '$':
		r.ReadByte()
		lenLine, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}
		length, err := strconv.Atoi(strings.TrimSpace(lenLine))
		if err != nil {
			return "", err
		}

		if length == -1 {
			return "$-1 (nil bulk string)", nil
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return "", err
		}
		if _, err := r.Discard(2); err != nil {
			return "", err
		}
		return fmt.Sprintf("Bulk(%d): %q", length, string(data)), nil

	case '*':
		r.ReadByte()
		lenLine, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}
		count, _ := strconv.Atoi(strings.TrimSpace(lenLine))
		return fmt.Sprintf("Array of %d elements", count), nil

	default:
		return "(binary / RDB data begins)", io.EOF
	}
}
