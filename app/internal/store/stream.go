package store

import (
	"fmt"
	"sync"
)

type StreamId string
type EntryId string
type StreamType struct {
	stream map[StreamId]map[EntryId][]Entry
	mu     sync.Mutex
}

type Entry struct {
	key   string
	value string
}

func (s *StreamType) Set(streamId string, entryId string, entries []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream := StreamId(streamId)
	id := EntryId(entryId)

	if _, ok := s.stream[stream]; !ok {
		s.stream[stream] = make(map[EntryId][]Entry)
	}
	if _, ok := s.stream[stream][id]; !ok {
		s.stream[stream][id] = []Entry{}
	}

	for i := 0; i < len(entries); i += 2 {
		s.stream[stream][id] = append(s.stream[stream][id],
			Entry{
				key:   entries[i],
				value: entries[i+1],
			})
	}
}

func (s *StreamType) ExistsStream(streamId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.stream[StreamId(streamId)]
	if !ok {
		return fmt.Errorf("stream %s not found", streamId)
	}

	return nil
}

func (s *StreamType) ValidateEntryId(streamId string, entryId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryId == "0-0" {
		return fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}

	stream := StreamId(streamId)
	id := EntryId(entryId)

	entryIds := s.stream[stream]

	lastEntryId := EntryId("")
	for entry := range entryIds {
		lastEntryId = entry
	}

	if id <= lastEntryId {
		return fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return nil
}
