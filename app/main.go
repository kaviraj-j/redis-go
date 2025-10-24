package main

import (
	"fmt"
	"log"
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
	replicaOf, ok := getFlagValue(args, replicaOfFlag)
	var masterServer MasterServer
	role := RoleMaster
	if ok {
		var err error
		masterServer, err = createConnectionWithMaster(replicaOf)
		if err != nil {
			log.Fatal(err)
		}
		role = RoleSlave
	}
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Failed to bind to port", port)
		os.Exit(1)
	}
	fmt.Println("Listening on port", port)
	app := newApp(l, role, masterServer)
	app.run()
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
