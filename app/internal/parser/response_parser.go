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

func EncodeNullArray() []byte {
	return []byte("*-1\r\n")
}

// EncodeArray returns a RESP array response
func EncodeArray(s []string) []byte {
	res := []byte(fmt.Sprintf("*%d\r\n", len(s)))
	for _, str := range s {
		res = append(res, EncodeBulkString(str)...)
	}
	return res
}

// EncodeError returns a RESP error reply.
func EncodeError(err error) []byte {
	return []byte("-" + err.Error() + "\r\n")
}

// Common error encoders
func EncodeWrongNumArgsError(cmd string) []byte {
	return EncodeError(fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd))
}

func EncodeInvalidIntError(field string) []byte {
	return EncodeError(fmt.Errorf("ERR value is not an integer or out of range"))
}
