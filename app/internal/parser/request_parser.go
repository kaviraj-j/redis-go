package parser

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Command struct {
	Name string
	Args []string
}

// ParseRequest reads a RESP command from the reader and parses it into a Command struct.
func ParseRequest(r *bufio.Reader) (*Command, int, error) {
	totalBytes := 0

	prefix, err := r.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, totalBytes, nil
		}
		return nil, totalBytes, err
	}
	totalBytes++

	if prefix != '*' {
		return nil, totalBytes, fmt.Errorf("expected '*', got '%c'", prefix)
	}

	line, n, err := readLineWithCount(r)
	if err != nil {
		return nil, totalBytes, err
	}
	totalBytes += n

	count, err := strconv.Atoi(line)
	if err != nil {
		return nil, totalBytes, fmt.Errorf("invalid array length: %v", err)
	}

	parts := make([]string, 0, count)
	for i := 0; i < count; i++ {
		bulkPrefix, err := r.ReadByte()
		if err != nil {
			return nil, totalBytes, err
		}
		totalBytes++
		if bulkPrefix != '$' {
			return nil, totalBytes, fmt.Errorf("expected '$', got '%c'", bulkPrefix)
		}

		lengthLine, n, err := readLineWithCount(r)
		if err != nil {
			return nil, totalBytes, err
		}
		totalBytes += n

		strLen, err := strconv.Atoi(lengthLine)
		if err != nil {
			return nil, totalBytes, fmt.Errorf("invalid bulk string length: %v", err)
		}

		buf := make([]byte, strLen)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, totalBytes, err
		}
		totalBytes += strLen

		if _, err := r.Discard(2); err != nil {
			return nil, totalBytes, err
		}
		totalBytes += 2

		parts = append(parts, string(buf))
	}

	if len(parts) == 0 {
		return nil, totalBytes, fmt.Errorf("empty command")
	}

	return &Command{
		Name: strings.ToUpper(parts[0]),
		Args: parts[1:],
	}, totalBytes, nil
}

func readLineWithCount(r *bufio.Reader) (string, int, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", 0, err
	}
	bytesConsumed := len(line)
	line = strings.TrimSuffix(line, "\r\n")
	line = strings.TrimSuffix(line, "\n")
	return line, bytesConsumed, nil
}
