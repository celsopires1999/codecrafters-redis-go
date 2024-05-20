package command

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/internal/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

func handleXread(h *Handler, userCommand *Command) error {
	var input struct {
		identifier string
		streamIds  []string
		entryIds   []string
	}

	type option int

	const (
		Single option = iota
		Multiple
	)

	var opt option
	switch len(userCommand.Args) {
	case 4:
		opt = Single
	default:
		opt = Multiple
		if len(userCommand.Args)%2 != 0 {
			return fmt.Errorf("the number of arguments for %s is incorrect", userCommand.Args[0])
		}
	}

	input.identifier = userCommand.Args[1]
	if opt == Single {
		input.streamIds = []string{userCommand.Args[2]}
		input.entryIds = []string{userCommand.Args[3]}
	} else {
		offset := 2
		size := len(userCommand.Args) - offset
		for i := 2; i < size/2+offset; i++ {
			input.streamIds = append(input.streamIds, userCommand.Args[i])
		}
		for i := size/2 + offset; i < size+offset; i++ {
			input.entryIds = append(input.entryIds, userCommand.Args[i])
		}
	}

	lstStreams := []encoder.ListStream{}
	for s := 0; s < len(input.streamIds); s++ {
		streamId := store.StreamId(input.streamIds[s])
		entryId, err := store.ToEntryId(input.entryIds[s], 0)
		if err != nil {
			return err
		}
		streamEntries := h.db.StreamType.FindGreater(streamId, entryId)
		lstEntries := []encoder.ListEntry{}
		for _, entry := range streamEntries {
			i := encoder.ListEntry{
				EntryId: entry.EntryId.String(),
				Facts:   store.ListEntriesFacts(entry.Facts),
			}
			lstEntries = append(lstEntries, i)
		}
		i := encoder.ListStream{
			StreamId: string(streamId),
			Entries:  lstEntries,
		}
		lstStreams = append(lstStreams, i)
	}
	h.WriteResponse(encoder.NewRead(lstStreams))
	return nil
}
