package store

import (
	"container/list"
	"fmt"
	"time"
)

type Store struct {
	storage map[string]Value
}

type Type string

const (
	ValueTypeString Type = "string"
	ValueTypeList   Type = "list"
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

func (StringValue) isValueType() {}
func (ListValue) isValueType()   {}

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
		storage: make(map[string]Value),
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
		existingListValue, ok := value.Data.(ListValue)
		if !ok || existingListValue.Data == nil {
			return 0, ErrInvalidData
		}
		listVal = existingListValue.Data
	}

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
