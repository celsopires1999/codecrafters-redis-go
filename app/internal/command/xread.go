package command

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/internal/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

func handleXread(h *Handler, userCommand *Command) error {
	var input struct {
		option   string
		streamId string
		entryId  string
	}

	if len(userCommand.Args) < 4 {
		return fmt.Errorf("the number of arguments for %s is incorrect", userCommand.Args[0])
	}

	input.option = userCommand.Args[1]
	input.streamId = userCommand.Args[2]
	input.entryId = userCommand.Args[3]

	streamId := store.StreamId(input.streamId)
	entryId, err := store.ToEntryId(input.entryId, 0)
	if err != nil {
		return err
	}

	streamEntries := h.db.StreamType.FindGreater(streamId, entryId)
	lstEntries := []encoder.ListEntry{}
	lstStreams := []encoder.ListStream{}
	for _, entry := range streamEntries {
		i := encoder.ListEntry{
			EntryId: entry.EntryId.String(),
			Values:  store.ListEntriesFacts(entry.Facts),
		}
		lstEntries = append(lstEntries, i)
	}
	i := encoder.ListStream{
		StreamId: string(streamId),
		Entries:  lstEntries,
	}
	lstStreams = append(lstStreams, i)
	h.WriteResponse(encoder.NewRead(lstStreams))
	return nil
}
