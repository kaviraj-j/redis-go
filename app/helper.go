package main

import (
	"crypto/rand"
	"encoding/hex"
)

const (
	replicationIdLength = 40
)

func generateReplicationId() string {
	id := make([]byte, replicationIdLength/2)
	if _, err := rand.Read(id); err != nil {
		panic(err)
	}
	return hex.EncodeToString(id)
}
