package store

import "time"

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

func (s *Store) SetString(key string, data string, expiry time.Time) {
	s.storage[key] = Value{
		Data:   StringValue(data),
		Type:   ValueTypeString,
		Expiry: expiry,
	}
}
