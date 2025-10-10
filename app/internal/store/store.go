package store

type Store struct {
	storage map[string]string
}

func NewStore() (*Store, error) {
	return &Store{
		storage: make(map[string]string),
	}, nil
}

func (s *Store) Set(key string, value string) {
	s.storage[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	val, ok := s.storage[key]
	return val, ok
}
