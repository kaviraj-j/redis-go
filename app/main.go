package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"slices"
)

const (
	portFlag    = "--port"
	defaultPort = "6379"
)

func main() {
	port := defaultPort
	args := os.Args
	// check if port flag is given
	portFlagIndex := slices.Index(args, portFlag)
	if portFlagIndex != -1 {
		if len(args) < portFlagIndex+2 {
			log.Fatal("provide port number")
		}
		port = args[portFlagIndex+1]
	}
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Failed to bind to port", port)
		os.Exit(1)
	}
	fmt.Println("Listening on port", port)
	app := newApp()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go app.handleConnection(conn)
	}
}
