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

func NewStore() (*Store, error) {
	return &Store{
		storage: make(map[string]Value),
	}, nil
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

	if exists && value.Type != ValueTypeList {
		return 0, fmt.Errorf("ERR invalid type for RPUSH operation")
	}

	var listVal *list.List

	if !exists || (!value.Expiry.IsZero() && value.Expiry.Before(time.Now())) {
		listVal = list.New()
	} else {
		existingListValue, ok := value.Data.(ListValue)
		if !ok || existingListValue.Data == nil {
			return 0, fmt.Errorf("ERR invalid list data")
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

func (s *Store) LRange(key string, start, end int) []string {
	values := make([]string, 0)
	value, exists := s.storage[key]

	// Check if key exists, is a list, and not expired
	if !exists || value.Type != ValueTypeList || (!value.Expiry.IsZero() && value.Expiry.Before(time.Now())) {
		return values
	}

	listValues, ok := value.Data.(ListValue)
	if !ok {
		return values
	}

	length := listValues.Data.Len()
	if length == 0 || start >= length {
		return values
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
		return values
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

	return values
}

func (s *Store) ListPop(key string, count int) []string {
	fmt.Println("count", count)

	value, listValues, exists := s.getList(key)
	if !exists {
		return []string{}
	}

	fmt.Println("litValues", listValues.Data)
	res := make([]string, 0, count)
	for i := 0; i < count && listValues.Data.Len() > 0; i++ {
		fmt.Println("inside the loop", i)
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
		value.Data = ListValue{
			Data: listValues.Data,
		}
		s.storage[key] = value
	}

	return res
}

func (s *Store) GetListLen(key string) int {
	value, exists := s.storage[key]

	// Check if key exists, is a list, and not expired
	if !exists || value.Type != ValueTypeList || (!value.Expiry.IsZero() && value.Expiry.Before(time.Now())) {
		return 0
	}
	listValues, ok := value.Data.(ListValue)
	if !ok {
		return 0
	}
	return listValues.Data.Len()
}

func (s *Store) Get(key string) (string, bool, error) {
	val, ok := s.storage[key]
	if !ok {
		return "", false, nil
	}
	if val.Type != ValueTypeString {
		return "", false, fmt.Errorf("ERR invalid type for command GET")
	}

	if !val.Expiry.IsZero() && val.Expiry.Before(time.Now()) {
		delete(s.storage, key)
		return "", false, nil
	}

	strVal, ok := val.Data.(StringValue)
	if !ok {
		return "", false, fmt.Errorf("ERR invalid string data")
	}

	return string(strVal), true, nil
}

func (s *Store) getList(key string) (Value, *ListValue, bool) {
	value, exists := s.storage[key]

	// Check if key exists, is a list, and not expired
	if !exists || value.Type != ValueTypeList || (!value.Expiry.IsZero() && value.Expiry.Before(time.Now())) {
		return value, nil, false
	}

	listValues, ok := value.Data.(ListValue)
	return value, &listValues, ok

}
