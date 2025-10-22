package main

import (
	"fmt"
	"net"
	"os"
	"slices"
)

const (
	portFlag      = "--port"
	replicaOfFlag = "--replicaof"
	defaultPort   = "6379"
)

func main() {
	port := defaultPort
	args := os.Args

	// check if port flag is given
	customPort, ok := getFlagValue(args, portFlag)
	if ok {
		port = customPort
	}

	config := Config{}
	replicaOf, ok := getFlagValue(args, replicaOfFlag)
	if ok {
		config.replicaOf = replicaOf
	}
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Failed to bind to port", port)
		os.Exit(1)
	}
	fmt.Println("Listening on port", port)
	app := newApp(config)
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go app.handleConnection(conn)
	}
}

func getFlagValue(args []string, flag string) (string, bool) {
	flagIndex := slices.Index(args, flag)
	if flagIndex == -1 {
		return "", false
	}
	if len(args) < flagIndex+2 {
		return "", false
	}
	return args[flagIndex+1], true
}
