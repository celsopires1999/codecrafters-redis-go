package store

import "fmt"

type StreamId string
type EntryId string
type Stream struct {
	m map[StreamId]map[EntryId][]Entry
}

type Entry struct {
	key   string
	value string
}

func NewStream() *Stream {
	return &Stream{
		m: make(map[StreamId]map[EntryId][]Entry),
	}
}

func (s *Stream) Set(streamId string, entryId string, entries []string) {
	stream := StreamId(streamId)
	id := EntryId(entryId)

	if _, ok := s.m[stream]; !ok {
		s.m[stream] = make(map[EntryId][]Entry)
	}
	if _, ok := s.m[stream][id]; !ok {
		s.m[stream][id] = []Entry{}
	}

	for i := 0; i < len(entries); i += 2 {
		s.m[stream][id] = append(s.m[stream][id],
			Entry{
				key:   entries[i],
				value: entries[i+1],
			})
	}
}

func (s *Stream) Exists(streamId string) error {
	_, ok := s.m[StreamId(streamId)]
	if !ok {
		return fmt.Errorf("stream %s not found", streamId)
	}

	return nil
}
