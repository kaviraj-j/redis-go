package store

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type XReadResponse struct {
	Key     string
	Entries []struct {
		Id       string
		KeyValue []string
	}
}

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

	// notify blocked clients
	go s.notifyStreamBlockers(key)

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
			if timestamp > startTimeStamp || (timestamp == startTimeStamp && seq > startSeq) {
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
		if len(entries) > 0 {
			res = append(res, XReadResponse{
				Key:     key,
				Entries: entries,
			})
		}

	}
	return res, nil
}

func (s *Store) XReadBlocked(keysMap map[string]string, waitForNewEntry bool) (<-chan []XReadResponse, error) {
	xReadChan := make(chan []XReadResponse, 1)
	var result []XReadResponse
	var err error
	if !waitForNewEntry {
		result, err = s.XRead(keysMap)
		if err != nil {
			return nil, err
		}
		if len(result) > 0 {
			xReadChan <- result
			return xReadChan, nil
		}
	} else {
		s.getLastIds(keysMap)
	}
	// no data available -> block and add to queue
	s.mu.Lock()
	for key, id := range keysMap {
		s.streamBlockedClients[key] = append(s.streamBlockedClients[key], StreamBlockedClients{
			id: id,
			ch: xReadChan,
		})
	}
	s.mu.Unlock()

	return xReadChan, nil
}

func (s *Store) getLastIds(keysMap map[string]string) {

	s.mu.Lock()
	// get the last element of all the keys
	for key := range keysMap {
		val, ok := s.storage[key]
		if !ok || s.deleteIfExpired(key, val) {
			ok = false
		}
		if err := s.validateType(val, ValueTypeStream); err != nil {
			ok = false
		}

		streamVal, _ := val.Data.(StreamValue)
		id := "0-0"
		if ok {
			id = streamVal.LastID
		}
		keysMap[key] = id
	}
	s.mu.Unlock()

}

func (s *Store) notifyStreamBlockers(key string) {
	s.mu.Lock()
	blockedClients, ok := s.streamBlockedClients[key]
	if !ok || len(blockedClients) == 0 {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	newBlockedClients := make([]StreamBlockedClients, 0)
	for _, client := range blockedClients {
		s.mu.Lock()
		res, err := s.XRead(map[string]string{key: client.id})
		if err != nil || len(res) == 0 {
			newBlockedClients = append(newBlockedClients, client)
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()

		client.ch <- res

		close(client.ch)

	}
	s.mu.Lock()
	if len(newBlockedClients) == 0 {
		delete(s.streamBlockedClients, key)
	} else {
		s.streamBlockedClients[key] = newBlockedClients
	}
	s.mu.Unlock()
}

func (s *Store) RemoveBlockedStreamChannel(key string, target <-chan []XReadResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()

	blockedClients, ok := s.streamBlockedClients[key]
	if !ok {
		return
	}

	newBlockedClients := make([]StreamBlockedClients, 0, len(blockedClients))
	for _, client := range blockedClients {
		if client.ch != target {
			newBlockedClients = append(newBlockedClients, client)
		}
	}
	if len(newBlockedClients) == 0 {
		delete(s.streamBlockedClients, key)
	} else {
		s.streamBlockedClients[key] = newBlockedClients
	}
}

func parseStreamID(id string) (int64, int64) {
	if id == "" {
		return 0, 0
	}
	var ts, seq int64
	fmt.Sscanf(id, "%d-%d", &ts, &seq)
	return ts, seq
}
