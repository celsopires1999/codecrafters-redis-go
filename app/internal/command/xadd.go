package command

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

func handleXadd(h *Handler, userCommand *Command) error {
	err := validateXaddCommand(userCommand)
	if err != nil {
		return err
	}

	streamId, entries := parseStreamIdAndEntries(userCommand)

	splitUserCommandEntryId := strings.Split(userCommand.Args[2], "-")

	var entryId store.EntryId

	switch {
	// user command does not specify an entry id
	case len(splitUserCommandEntryId) == 1 && splitUserCommandEntryId[0] == "*":
		entryId, err = h.db.GenerateId(streamId, "")
		if err != nil {
			return err
		}
	// user command specifies milli part of entry id
	case len(splitUserCommandEntryId) == 2 && splitUserCommandEntryId[1] == "*":
		entryId, err = h.db.GenerateId(streamId, splitUserCommandEntryId[0])
		if err != nil {
			return err
		}
	// user command specifies entry id
	default:
		sequence, err := strconv.Atoi(splitUserCommandEntryId[1])
		if err != nil {
			return err
		}
		entryId = store.NewEntryId(splitUserCommandEntryId[0], sequence)
		if err := h.db.StreamType.ValidateEntryId(streamId, entryId); err != nil {
			h.WriteResponse(encoder.NewError(err.Error()))
			return nil
		}
	}

	h.db.StreamType.Set(streamId, entryId, entries)
	ps.Publish("xadd", string(streamId))

	h.WriteResponse(encoder.NewString(entryId.String()))

	return nil
}

func validateXaddCommand(userCommand *Command) error {
	// Must have at least 5 arguments
	if len(userCommand.Args) < 5 {
		return fmt.Errorf("the number of arguments for %s is incorrect", userCommand.Args[0])
	}

	// Entries must be an even number
	if len(userCommand.Args[3:])%2 != 0 {
		return fmt.Errorf("the number of arguments for %s is incorrect", userCommand.Args[0])
	}

	return nil
}

func parseStreamIdAndEntries(userCommand *Command) (store.StreamId, []store.Fact) {
	streamId := store.StreamId(userCommand.Args[1])

	var entries []store.Fact
	for i := 3; i < len(userCommand.Args); i += 2 {
		entries = append(entries,
			store.NewFact(userCommand.Args[i], userCommand.Args[i+1]))
	}
	return streamId, entries
}
