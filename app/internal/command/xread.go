package command

import (
	"fmt"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

func handleXread(h *Handler, userCommand *Command) error {
	var input struct {
		identifier string
		streamIds  []string
		entryIds   []string
		hasBlock   bool
		blockTime  int
	}

	type option int
	const (
		StreamsSingle option = iota
		StreamsMultiple
		Block
		BlockWithTimeout
	)
	var opt option

	switch userCommand.Args[1] {
	case "streams":
		if len(userCommand.Args) == 4 {
			opt = StreamsSingle
		} else {
			opt = StreamsMultiple
			if len(userCommand.Args)%2 != 0 {
				return fmt.Errorf("the number of arguments for %s is incorrect", userCommand.Args[0])
			}
		}
	case "block":
		if userCommand.Args[3] != "streams" {
			return fmt.Errorf("invalid command arguments for %s command", userCommand.Args[0])
		}
		if userCommand.Args[2] == "0" || userCommand.Args[2] == "\\x00" {
			opt = Block
		} else {
			opt = BlockWithTimeout
		}
		input.hasBlock = true
		input.blockTime, _ = strconv.Atoi(userCommand.Args[2])
		userCommand.Args = append(userCommand.Args[0:1], userCommand.Args[3:]...)
	default:
		return fmt.Errorf("invalid command arguments for %s command", userCommand.Args[0])
	}

	input.identifier = userCommand.Args[1]

	if opt == StreamsMultiple {
		offset := 2
		size := len(userCommand.Args) - offset
		for i := 2; i < size/2+offset; i++ {
			input.streamIds = append(input.streamIds, userCommand.Args[i])
		}
		for i := size/2 + offset; i < size+offset; i++ {
			input.entryIds = append(input.entryIds, userCommand.Args[i])
		}
	} else {
		input.streamIds = []string{userCommand.Args[2]}
		input.entryIds = []string{userCommand.Args[3]}
	}

	if opt == Block {
		doBlock(input.streamIds[0])
	}

	if opt == BlockWithTimeout {
		err := doBlockWithTimeout(h, input.streamIds[0], input.blockTime)
		if err != nil {
			return err
		}
	}

	return writeXreadResponse(h, input.streamIds, input.entryIds)
}

func doBlockWithTimeout(h *Handler, streamId string, blockTime int) error {
	for {
		ch := ps.Subscribe("xadd")
		defer ps.Unsubscribe("xadd")
		select {
		case event := <-ch:
			if event.Message == streamId {
				return nil
			}
		case <-time.After(time.Duration(blockTime) * time.Millisecond):
			h.WriteResponse(encoder.Null)
			return nil
		}
	}
}

func doBlock(streamId string) {

	ch := ps.Subscribe("xadd")
	defer ps.Unsubscribe("xadd")
	for {
		event := <-ch
		if event.Message == streamId {
			return
		}
	}
}

func writeXreadResponse(h *Handler, streamIds []string, entryIds []string) error {
	lstStreams := []encoder.ListStream{}
	for s := 0; s < len(streamIds); s++ {
		streamId := store.StreamId(streamIds[s])
		entryId, err := store.ToEntryId(entryIds[s], 0)
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
