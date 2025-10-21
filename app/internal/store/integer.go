package store

import (
	"fmt"
	"strconv"
	"time"
)

func (s *Store) Increment(key string) (int, error) {
	val, ok := s.storage[key]
	if !ok || s.deleteIfExpired(key, val) {
		s.SetString(key, "1", time.Time{})
		return 1, nil
	}
	err := s.validateType(val, ValueTypeString)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer or out of range")
	}
	valStr, _ := val.Data.(StringValue)
	valInt, err := strconv.Atoi(string(valStr))
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer or out of range")
	}
	valInt++
	s.SetString(key, strconv.Itoa(valInt), time.Time{})
	return valInt, nil
}
