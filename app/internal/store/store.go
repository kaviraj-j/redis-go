package store

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type Store struct {
	storage             map[string]Value
	listBlockChannels   map[string][]chan string
	streamBlockChannels map[string][]chan []XReadResponse
	mu                  sync.Mutex
}

type Type string

const (
	ValueTypeString Type = "string"
	ValueTypeList   Type = "list"
	ValueTypeStream Type = "stream"
)

type ValueType interface {
	isValueType()
}

type StringValue string

type ListValue struct {
	Data *list.List
}

type PushListDirection string

const (
	RPush PushListDirection = "RPUSH"
	LPush PushListDirection = "LPUSH"
)

type StreamValue struct {
	Entries  []StreamEntry
	EntryMap map[string]*StreamEntry
	LastID   string
}

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

func (StringValue) isValueType() {}
func (ListValue) isValueType()   {}
func (StreamValue) isValueType() {}

type Value struct {
	Data   ValueType
	Type   Type
	Expiry time.Time
}

var (
	ErrWrongType   = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrInvalidData = fmt.Errorf("ERR invalid data structure")
)

func NewStore() (*Store, error) {
	return &Store{
		storage:             make(map[string]Value),
		listBlockChannels:   make(map[string][]chan string),
		streamBlockChannels: make(map[string][]chan []XReadResponse),
	}, nil
}

func (s *Store) GetType(key string) string {
	val, ok := s.storage[key]
	if !ok {
		return "none"
	}
	return string(val.Type)
}
