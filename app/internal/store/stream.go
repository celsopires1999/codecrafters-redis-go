package store

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Entry struct {
	EntryId EntryId
	Facts   []Fact
}

type EntryId struct {
	milli    string
	sequence int
}

func NewEntryId(milli string, sequence int) EntryId {
	return EntryId{milli: milli, sequence: sequence}
}

func ToEntryId(s string, defaultSequence int) (EntryId, error) {
	parts := strings.Split(s, "-")

	milli := parts[0]
	sequence := defaultSequence
	var err error

	if len(parts) == 2 {
		sequence, err = strconv.Atoi(parts[1])
		if err != nil {
			return EntryId{}, err
		}
	}

	return EntryId{milli, sequence}, nil
}

func (e EntryId) String() string {
	return fmt.Sprintf("%s-%d", e.milli, e.sequence)
}

// Compare returns -1 if e < other, 1 if e > other, and 0 if e == other
func (e EntryId) Compare(other EntryId) int {
	if e.milli > other.milli {
		return 1
	}
	if e.milli < other.milli {
		return -1
	}
	if e.sequence > other.sequence {
		return 1
	}
	if e.sequence < other.sequence {
		return -1
	}
	return 0
}

type Fact struct {
	key   string
	value string
}

func NewFact(key, value string) Fact {
	return Fact{key: key, value: value}
}

func (e Fact) GetKV() []string {
	return []string{e.key, e.value}
}

type StreamId string

type StreamType struct {
	stream map[StreamId]map[EntryId][]Fact
	mu     sync.Mutex
}

func (s *StreamType) Set(streamId StreamId, entryId EntryId, entries []Fact) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.stream[streamId]; !ok {
		s.stream[streamId] = make(map[EntryId][]Fact)
	}
	if _, ok := s.stream[streamId][entryId]; !ok {
		s.stream[streamId][entryId] = []Fact{}
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

	slices.SortFunc(keys, func(a, b EntryId) int {
		if n := cmp.Compare(a.milli, b.milli); n != 0 {
			return n
		}
		// If millis are equal, order by sequence
		return cmp.Compare(a.sequence, b.sequence)
	})
	return keys
}

func (s *StreamType) FindGreater(streamId StreamId, entry EntryId) (output []Entry) {
	for _, entryId := range s.GetEntryIds(streamId) {
		if entryId.Compare(entry) > 0 {
			output = append(output, Entry{entryId, s.stream[streamId][entryId]})
		}
	}

	return
}

func (s *StreamType) FindStarEnd(streamId StreamId, start EntryId, end EntryId) (output []Entry) {
	for _, entryId := range s.GetEntryIds(streamId) {
		if entryId.Compare(start) >= 0 && entryId.Compare(end) <= 0 {
			output = append(output, Entry{entryId, s.stream[streamId][entryId]})
		}
	}

	return
}

func (s *StreamType) FindLastEntryId(streamId StreamId) (lastEntryId EntryId) {
	for entry := range s.stream[streamId] {
		if lastEntryId.Compare(entry) == -1 {
			lastEntryId = entry
		}
	}
	return
}

func ListEntriesFacts(entries []Fact) (output []string) {
	for _, entry := range entries {
		output = append(output, entry.GetKV()...)
	}
	return
}
