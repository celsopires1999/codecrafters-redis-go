package command

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

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

func parseStreamIdAndEntries(userCommand *Command) (store.StreamId, []store.Entry) {
	streamId := store.StreamId(userCommand.Args[1])

	var entries []store.Entry
	for i := 3; i < len(userCommand.Args); i += 2 {
		entries = append(entries,
			store.NewEntry(userCommand.Args[i], userCommand.Args[i+1]))
	}
	return streamId, entries
}
