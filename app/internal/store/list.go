package store

import "container/list"

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

func (s *Store) RemoveBlockedListChannel(key string, ch <-chan string) {
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
