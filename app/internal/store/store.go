package store

import "time"

type Store struct {
	storage map[string]value
}

type value struct {
	valueStr string
	expiry   time.Time
}

func NewStore() (*Store, error) {
	return &Store{
		storage: make(map[string]value),
	}, nil
}

func (s *Store) Set(key string, valueStr string, expiry time.Time) {

	s.storage[key] = value{
		valueStr: valueStr,
		expiry:   expiry,
	}
}

func (s *Store) Get(key string) (string, bool) {
	val, ok := s.storage[key]
	if !ok {
		return "", false
	}
	if val.expiry.IsZero() || val.expiry.After(time.Now()) {
		return val.valueStr, true
	}
	delete(s.storage, key)
	return "", false
}
