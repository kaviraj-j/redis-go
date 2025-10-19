package store

import (
	"container/list"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Store struct {
	storage           map[string]Value
	listBlockChannels map[string][]chan string
	mu                sync.Mutex
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

// ---- Response Return Types ----
type XReadResponse struct {
	Key     string
	Entries []struct {
		Id       string
		KeyValue []string
	}
}

var (
	ErrWrongType   = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrInvalidData = fmt.Errorf("ERR invalid data structure")
)

func NewStore() (*Store, error) {
	return &Store{
		storage:           make(map[string]Value),
		listBlockChannels: make(map[string][]chan string),
	}, nil
}

// isExpired checks if a value has expired
func (s *Store) isExpired(value Value) bool {
	return !value.Expiry.IsZero() && value.Expiry.Before(time.Now())
}

// deleteIfExpired removes the key if it has expired
func (s *Store) deleteIfExpired(key string, value Value) bool {
	if s.isExpired(value) {
		delete(s.storage, key)
		return true
	}
	return false
}

// validateType checks if the value type matches expected type
func (s *Store) validateType(value Value, expectedType Type) error {
	if value.Type != expectedType {
		return ErrWrongType
	}
	return nil
}

func (s *Store) SetString(key string, data string, expiry time.Time) {
	s.storage[key] = Value{
		Data:   StringValue(data),
		Type:   ValueTypeString,
		Expiry: expiry,
	}
}

func (s *Store) PushList(key string, data []string, direction PushListDirection) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, exists := s.storage[key]
	if exists {
		if err := s.validateType(value, ValueTypeList); err != nil {
			return 0, err
		}
	}

	var listVal *list.List
	if !exists || s.isExpired(value) {
		listVal = list.New()
	} else {
		existing, ok := value.Data.(ListValue)
		if !ok || existing.Data == nil {
			return 0, ErrInvalidData
		}
		listVal = existing.Data
	}

	// Push data
	if direction == RPush {
		for _, item := range data {
			listVal.PushBack(item)
		}
	} else {
		for _, item := range data {
			listVal.PushFront(item)
		}
	}

	s.storage[key] = Value{
		Data:   ListValue{Data: listVal},
		Type:   ValueTypeList,
		Expiry: value.Expiry,
	}

	go func() {
		// Wake up blocked clients (FIFO)
		if chans, ok := s.listBlockChannels[key]; ok && len(chans) > 0 {
			for len(chans) > 0 && listVal.Len() > 0 {
				ch := chans[0]
				chans = chans[1:]
				front := listVal.Front()
				str, _ := front.Value.(string)
				listVal.Remove(front)
				ch <- str
			}
			s.listBlockChannels[key] = chans
		}
	}()
	return listVal.Len(), nil
}

func (s *Store) LRange(key string, start, end int) ([]string, error) {
	values := make([]string, 0)
	value, exists := s.storage[key]

	if !exists {
		return values, nil
	}

	if err := s.validateType(value, ValueTypeList); err != nil {
		return nil, err
	}

	if s.deleteIfExpired(key, value) {
		return values, nil
	}

	listValues, ok := value.Data.(ListValue)
	if !ok {
		return nil, ErrInvalidData
	}

	length := listValues.Data.Len()
	if length == 0 || start >= length {
		return values, nil
	}

	// Normalize start index
	if start < 0 {
		start = max(length+start, 0)
	}

	// Normalize end index
	if end < 0 {
		end = length + end
	} else if end >= length {
		end = length - 1
	}

	// Validate range
	if start > end {
		return values, nil
	}

	// Iterate to the start position
	element := listValues.Data.Front()
	for i := 0; i < start && element != nil; i++ {
		element = element.Next()
	}

	// Collect values in range
	for i := start; i <= end && element != nil; i++ {
		if str, ok := element.Value.(string); ok {
			values = append(values, str)
		}
		element = element.Next()
	}

	return values, nil
}

func (s *Store) ListPop(key string, count int) ([]string, error) {
	value, exists := s.storage[key]

	if !exists {
		return []string{}, nil
	}

	if err := s.validateType(value, ValueTypeList); err != nil {
		return nil, err
	}

	if s.deleteIfExpired(key, value) {
		return []string{}, nil
	}

	listValues, ok := value.Data.(ListValue)
	if !ok {
		return nil, ErrInvalidData
	}

	res := make([]string, 0, count)
	for i := 0; i < count && listValues.Data.Len() > 0; i++ {
		el := listValues.Data.Front()
		if el == nil {
			break
		}
		str, ok := el.Value.(string)
		if ok {
			res = append(res, str)
		}
		listValues.Data.Remove(el)
	}

	// Update store if list becomes empty
	if listValues.Data.Len() == 0 {
		delete(s.storage, key)
	} else {
		value.Data = ListValue{Data: listValues.Data}
		s.storage[key] = value
	}

	return res, nil
}

func (s *Store) BlockListPop(key string) (<-chan string, error) {
	doneChan := make(chan string, 1)

	s.mu.Lock()
	defer s.mu.Unlock()

	value, exists := s.storage[key]
	if exists {
		if err := s.validateType(value, ValueTypeList); err != nil {
			return nil, err
		}

		s.deleteIfExpired(key, value)

		listVal, ok := value.Data.(ListValue)
		if !ok {
			return nil, ErrInvalidData
		}

		if listVal.Data.Len() > 0 {
			// Pop immediately
			front := listVal.Data.Front()
			str, _ := front.Value.(string)
			doneChan <- str
			listVal.Data.Remove(front)

			s.storage[key] = Value{
				Data:   listVal,
				Type:   ValueTypeList,
				Expiry: value.Expiry,
			}
			return doneChan, nil
		}
	}

	// No element — block
	s.listBlockChannels[key] = append(s.listBlockChannels[key], doneChan)
	return doneChan, nil
}

func (s *Store) GetListLen(key string) (int, error) {
	value, exists := s.storage[key]

	if !exists {
		return 0, nil
	}

	if err := s.validateType(value, ValueTypeList); err != nil {
		return 0, err
	}

	if s.deleteIfExpired(key, value) {
		return 0, nil
	}

	listValues, ok := value.Data.(ListValue)
	if !ok {
		return 0, ErrInvalidData
	}
	return listValues.Data.Len(), nil
}

// ======== stream =========
func (s *Store) XAdd(key string, id string, fields map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, exists := s.storage[key]
	if exists {
		if err := s.validateType(value, ValueTypeStream); err != nil {
			return "", err
		}
		// if s.deleteIfExpired(key, value) {
		// 	exists = false
		// }
	}

	var stream StreamValue
	if !exists {
		stream = StreamValue{
			Entries:  make([]StreamEntry, 0),
			EntryMap: make(map[string]*StreamEntry),
		}
	} else {
		existing, ok := value.Data.(StreamValue)
		if !ok {
			return "", ErrInvalidData
		}
		stream = existing
	}
	idParts := strings.Split(id, "-")
	if len(idParts) == 2 && idParts[1] == "*" {
		timestamp, err := strconv.Atoi(idParts[0])
		if err != nil {
			return "", err
		}

		lastTime, lastSeq := parseStreamID(stream.LastID)
		if lastTime > int64(timestamp) {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if lastTime == int64(timestamp) {
			lastSeq++
		} else {
			lastSeq = 0
		}
		id = fmt.Sprintf("%d-%d", timestamp, lastSeq)

	}
	// --- Generate ID fully ---
	if id == "*" {
		nowMillis := time.Now().UnixNano() / int64(time.Millisecond)

		lastTime, lastSeq := parseStreamID(stream.LastID)
		if nowMillis == lastTime {
			// same millisecond — increment sequence
			lastSeq++
		} else {
			// new millisecond — reset sequence
			lastSeq = 0
		}
		id = fmt.Sprintf("%d-%d", nowMillis, lastSeq)
	}

	// --- Prevent Invalid entries ---
	if id == "0-0" {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}
	if _, exists := stream.EntryMap[id]; exists || stream.LastID >= id {
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	// --- Append entry ---
	entry := StreamEntry{
		ID:     id,
		Fields: fields,
	}
	stream.Entries = append(stream.Entries, entry)
	stream.EntryMap[id] = &stream.Entries[len(stream.Entries)-1]
	stream.LastID = id

	s.storage[key] = Value{
		Data: stream,
		Type: ValueTypeStream,
	}

	return id, nil
}

func (s *Store) XRange(key string, start string, end string) ([][]string, error) {
	val, ok := s.storage[key]
	if !ok {
		return [][]string{}, nil
	}
	if err := s.validateType(val, ValueTypeStream); err != nil {
		return [][]string{}, err
	}

	if s.deleteIfExpired(key, val) {
		return [][]string{}, nil
	}

	streamVal, _ := val.Data.(StreamValue)

	// Handle special cases: "-" means smallest, "+" means largest
	startTimeStamp, startSeq := int64(0), int64(0)
	if start != "-" {
		startTimeStamp, startSeq = parseStreamID(start)
	}

	endTimeStamp, endSeq := int64(1<<63-1), int64(1<<63-1)
	if end != "+" {
		endTimeStamp, endSeq = parseStreamID(end)
	}

	results := [][]string{}

	for _, entry := range streamVal.Entries {
		ts, seq := parseStreamID(entry.ID)

		if (ts > startTimeStamp || (ts == startTimeStamp && seq >= startSeq)) &&
			(ts < endTimeStamp || (ts == endTimeStamp && seq <= endSeq)) {

			arr := []string{entry.ID}
			for field, val := range entry.Fields {
				arr = append(arr, field, val)
			}
			results = append(results, arr)
		}
	}

	return results, nil
}

func (s *Store) XRead(keysMap map[string]string) ([]XReadResponse, error) {
	res := make([]XReadResponse, 0, len(keysMap))
	for key, id := range keysMap {
		entries := []struct {
			Id       string
			KeyValue []string
		}{}

		val, ok := s.storage[key]
		if !ok || s.deleteIfExpired(key, val) {
			continue
		}
		if err := s.validateType(val, ValueTypeStream); err != nil {
			return []XReadResponse{}, err
		}

		streamVal, _ := val.Data.(StreamValue)
		startTimeStamp, startSeq := parseStreamID(id)
		for _, entry := range streamVal.Entries {
			timestamp, seq := parseStreamID(entry.ID)
			if timestamp > startTimeStamp || (timestamp == startTimeStamp && seq >= startSeq) {
				fields := make([]string, 0, len(entry.Fields)*2)
				for k, v := range entry.Fields {
					fields = append(fields, k, v)
				}
				entries = append(entries, struct {
					Id       string
					KeyValue []string
				}{
					Id:       entry.ID,
					KeyValue: fields,
				})
			}
		}
		res = append(res, XReadResponse{
			Key:     key,
			Entries: entries,
		})

	}
	return res, nil
}

func parseStreamID(id string) (int64, int64) {
	if id == "" {
		return 0, 0
	}
	var ts, seq int64
	fmt.Sscanf(id, "%d-%d", &ts, &seq)
	return ts, seq
}

func (s *Store) Get(key string) (string, bool, error) {
	val, ok := s.storage[key]
	if !ok {
		return "", false, nil
	}

	if err := s.validateType(val, ValueTypeString); err != nil {
		return "", false, err
	}

	if s.deleteIfExpired(key, val) {
		return "", false, nil
	}

	strVal, ok := val.Data.(StringValue)
	if !ok {
		return "", false, ErrInvalidData
	}

	return string(strVal), true, nil
}

func (s *Store) GetType(key string) string {
	val, ok := s.storage[key]
	if !ok {
		return "none"
	}
	return string(val.Type)
}

func (s *Store) RemoveBlockedChannel(key string, ch <-chan string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	chans := s.listBlockChannels[key]
	for i, c := range chans {
		if c == ch {
			// Remove from slice
			s.listBlockChannels[key] = append(chans[:i], chans[i+1:]...)
			break
		}
	}
	// Cleanup empty entry
	if len(s.listBlockChannels[key]) == 0 {
		delete(s.listBlockChannels, key)
	}
}
