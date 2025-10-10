package parser

import (
	"fmt"
)

// EncodeSimpleString returns a RESP Simple String reply.
func EncodeString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

// EncodeBulkString returns a RESP Bulk String reply.
func EncodeBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}
