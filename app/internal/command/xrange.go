package command

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/internal/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

func handleXrange(h *Handler, userCommand *Command) error {

	var input struct {
		streamId string
		start    string
		end      string
	}

	if len(userCommand.Args) < 4 {
		return fmt.Errorf("the number of arguments for %s is incorrect", userCommand.Args[0])
	}

	input.streamId = userCommand.Args[1]
	input.start = userCommand.Args[2]
	input.end = userCommand.Args[3]

	streamId := store.StreamId(input.streamId)

	if input.start == "-" {
		input.start = "0"
	}

	if input.end == "+" {
		input.end = "9999999999999"
	}

	start, err := store.ToEntryId(input.start, 0)
	if err != nil {
		return err
	}
	end, err := store.ToEntryId(input.end, int(^uint(0)>>1))
	if err != nil {
		return err
	}
	streamEntries := h.db.StreamType.FindStarEnd(streamId, start, end)
	lstEntries := []encoder.ListEntry{}

	for _, v := range streamEntries {
		xrange := encoder.ListEntry{
			EntryId: v.EntryId.String(),
			Values:  store.ListEntriesFacts(v.Facts),
		}
		lstEntries = append(lstEntries, xrange)
	}

	h.WriteResponse(encoder.NewList(lstEntries))
	return nil
}
