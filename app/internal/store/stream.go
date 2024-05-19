package store

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type StreamId string
type EntryId struct {
	milli    string
	sequence int
}

func NewEntryId(milli string, sequence int) EntryId {
	return EntryId{milli: milli, sequence: sequence}
}

func (e EntryId) String() string {
	return fmt.Sprintf("%s-%d", e.milli, e.sequence)
}

type Entry struct {
	key   string
	value string
}

func NewEntry(key, value string) Entry {
	return Entry{key: key, value: value}
}

type StreamType struct {
	stream map[StreamId]map[EntryId][]Entry
	mu     sync.Mutex
}

func (s *StreamType) Set(streamId StreamId, entryId EntryId, entries []Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.stream[streamId]; !ok {
		s.stream[streamId] = make(map[EntryId][]Entry)
	}
	if _, ok := s.stream[streamId][entryId]; !ok {
		s.stream[streamId][entryId] = []Entry{}
	}

	s.stream[streamId][entryId] = append(s.stream[streamId][entryId],
		entries...)
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

func (s *StreamType) ValidateEntryId(streamId StreamId, entryId EntryId) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryId.milli == "0" && entryId.sequence == 0 {
		return fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}

	entryIds := s.GetEntryIds(streamId)

	lastEntryId := EntryId{}
	for _, entry := range entryIds {
		lastEntryId = entry
	}

	if entryId.milli > lastEntryId.milli {
		return nil
	}

	if entryId.milli == lastEntryId.milli && entryId.sequence > lastEntryId.sequence {
		return nil
	}

	return fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
}

func (s *StreamType) GenerateId(streamId StreamId, milli string) (EntryId, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if milli == "" {
		timestamp := uint64(time.Now().UnixMilli())
		milli = fmt.Sprintf("%d", timestamp)
	}

	entryIDs := s.GetEntryIds(streamId)

	sequence := 0
	foundMilli := false
	for _, entryID := range entryIDs {
		if entryID.milli == milli {
			sequence = entryID.sequence
			foundMilli = true
		}
		if entryID.milli > milli {
			break
		}
	}

	if foundMilli {
		sequence++
	} else if milli == "0" {
		sequence = 1
	}

	return EntryId{milli, sequence}, nil
}

func (s *StreamType) GetEntryIds(streamId StreamId) []EntryId {
	keys := make([]EntryId, 0, len(s.stream[streamId]))
	for k := range s.stream[streamId] {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].milli == keys[j].milli {
			return keys[i].sequence < keys[j].sequence
		}
		return keys[i].milli < keys[j].milli
	})

	return keys
}
