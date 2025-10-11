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

func (s *Store) RpushList(key string, data string, expiry time.Time) (int, error) {
	value, exists := s.storage[key]

	if exists && value.Type != ValueTypeList {
		return 0, fmt.Errorf("ERR invalid type for RPUSH operation")
	}

	var listVal *list.List

	if !exists || (value.Expiry != (time.Time{}) && value.Expiry.Before(time.Now())) {
		listVal = list.New()
	} else {
		existingListValue, ok := value.Data.(ListValue)
		if !ok || existingListValue.Data == nil {
			return 0, fmt.Errorf("ERR invalid list data")
		}
		listVal = existingListValue.Data
	}

	listVal.PushBack(data)

	s.storage[key] = Value{
		Data:   ListValue{Data: listVal},
		Type:   ValueTypeList,
		Expiry: expiry,
	}

	return listVal.Len(), nil
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
