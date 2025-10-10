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
func ParseRequest(r *bufio.Reader) (*Command, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}

	if prefix != '*' {
		return nil, fmt.Errorf("expected '*', got '%c'", prefix)
	}

	// Read number of array elements
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %v", err)
	}

	parts := make([]string, 0, count)
	for i := 0; i < count; i++ {
		bulkPrefix, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if bulkPrefix != '$' {
			return nil, fmt.Errorf("expected '$', got '%c'", bulkPrefix)
		}

		lengthLine, err := readLine(r)
		if err != nil {
			return nil, err
		}
		strLen, err := strconv.Atoi(lengthLine)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %v", err)
		}

		buf := make([]byte, strLen)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		// Consume trailing CRLF
		if _, err := r.Discard(2); err != nil {
			return nil, err
		}

		parts = append(parts, string(buf))
	}

	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	return &Command{
		Name: strings.ToUpper(parts[0]),
		Args: parts[1:],
	}, nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")
	line = strings.TrimSuffix(line, "\n")
	return line, nil
}
