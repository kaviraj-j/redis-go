package store

import "time"

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
