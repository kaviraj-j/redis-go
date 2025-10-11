package parser

import (
	"fmt"
)

// EncodeSimpleString returns a RESP Simple String reply.
func EncodeString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

// EncodeInt returns a RESP integer reply
func EncodeInt(n int) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", n))
}

// EncodeBulkString returns a RESP Bulk String reply.
func EncodeBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

// EncodeNullBulkString returns a null response
func EncodeNullBulkString() []byte {
	return []byte("$-1\r\n")
}
