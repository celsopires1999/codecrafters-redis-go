package command

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/config"
	"github.com/codecrafters-io/redis-starter-go/app/internal/encoder"
	"github.com/codecrafters-io/redis-starter-go/rdb"
)

func handlePing(h *Handler, _ *Command) error {
	pingMsg := encoder.Pong
	h.WriteResponse(pingMsg)
	return nil
}

func handleEcho(h *Handler, userCommand *Command) error {
	if len(userCommand.Args) < 2 {
		return fmt.Errorf("%s command requires an argument", strings.ToUpper(Echo))
	}

	arg := userCommand.Args[1]
	h.WriteResponse(encoder.NewBulkString(arg))
	return nil
}

func handleGet(h *Handler, userCommand *Command) error {
	if len(userCommand.Args) < 2 {
		return fmt.Errorf("%s command requires a key argument", strings.ToUpper(Get))
	}

	key := userCommand.Args[1]
	value, err := h.db.Get(key)
	if err != nil {
		h.writer.WriteString(encoder.Null)
	} else {
		h.writer.WriteString(encoder.NewBulkString(value))
	}
	return nil
}

func handleSet(h *Handler, userCommand *Command) error {
	if len(userCommand.Args) < 3 {
		return fmt.Errorf("%s command requires at least key and value arguments", strings.ToUpper(Set))
	}
	if len(userCommand.Args) > 5 {
		return fmt.Errorf("invalid command arguments")
	}

	key, value := userCommand.Args[1], userCommand.Args[2]
	expTime := int64(0)
	expires := false

	if len(userCommand.Args) == 5 {
		expInstruction := strings.ToLower(userCommand.Args[3])
		if expInstruction != Px {
			return fmt.Errorf("the command %s only allows the %s as a complimenting command",
				strings.ToUpper(Set), strings.ToUpper(Px))
		}

		time := userCommand.Args[4]
		var err error
		expires = true
		expTime, err = strconv.ParseInt(time, 10, 64)
		if err != nil {
			return fmt.Errorf("the argument after %s should be an integer number", strings.ToUpper(Px))
		}
	}

	h.db.Set(key, value, expires, expTime)
	if h.cfg.Role() == config.RoleMaster {
		wg := sync.WaitGroup{}
		command := encoder.NewArray(userCommand.Args)
		for _, slave := range h.cfg.Slaves() {
			wg.Add(1)
			go slave.PropagateCommand(command, &wg)
		}
		wg.Wait()
		h.UpdateSlavesOffset(len([]byte(command)))
	}
	h.WriteResponse(encoder.Ok)

	return nil
}

func handleInfo(h *Handler, userCommand *Command) error {
	infoOf := strings.ToLower(userCommand.Args[1])
	switch infoOf {
	default:
		return fmt.Errorf("%s is an invalid argument", infoOf)
	case Replication:
		info := strings.Join(
			[]string{
				fmt.Sprintf("role:%s", h.cfg.Role()),
				fmt.Sprintf("master_replid:%s", h.cfg.ReplID()),
				fmt.Sprintf("master_repl_offset:%d", h.cfg.ReplOffset()),
			},
			"\n",
		)
		h.writer.WriteString(encoder.NewBulkString(info))
	}

	return nil
}

func handleReplconf(h *Handler, userCommand *Command) error {
	confOf := strings.ToLower(userCommand.Args[1])
	switch confOf {
	default:
		h.WriteResponse(encoder.Ok)
	case GetAck:
		if h.cfg.Role() == config.RoleMaster {
			info := strings.ToUpper(strings.Join(userCommand.Args, " "))
			return fmt.Errorf("the %s command is only available for slaves", info)
		}

		offSet := strconv.Itoa(h.cfg.ReplOffset())
		response := encoder.NewArray([]string{
			userCommand.Args[0],
			strings.ToUpper(Ack),
			offSet,
		},
		)
		h.writer.WriteString(response)
	case Ack:
		if h.cfg.Role() == config.RoleSlave {
			info := strings.ToUpper(strings.Join(userCommand.Args, " "))
			return fmt.Errorf("the %s command is only available for master", info)
		}
		//offSet, _ := strconv.Atoi(userCommand.Args[2])
		h.NotifyAckSlaves()
	}

	return nil
}

func handleConfig(h *Handler, userCommand *Command) error {
	config := strings.ToLower(userCommand.Args[1])
	switch config {
	default:
		return fmt.Errorf("%s is an invalid argument", strings.ToUpper(config))
	case Get:
		for _, arg := range userCommand.Args[2:] {
			configOf := strings.ToLower(arg)
			if configOf == Dir {
				dir := h.cfg.Dir()
				h.WriteResponse(encoder.NewArray([]string{configOf, dir}))
			}
			if configOf == DBfilename {
				fileName := h.cfg.RDBFileName()
				h.WriteResponse(encoder.NewArray([]string{configOf, fileName}))
			}
		}
	}
	return nil
}

func handlePsync(h *Handler, _ *Command) error {
	h.writer.WriteString(
		encoder.NewString(
			fmt.Sprintf("%s %s %d", encoder.Fullsync, "REPL_ID", 0),
		),
	)

	dbData, err := rdb.GetEmptyRDBContent()
	if err != nil {
		return fmt.Errorf("failed to read rdb file, error: %w", err)
	}
	h.writer.WriteString(encoder.NewRDBFile(dbData))

	slave := config.NewSlave(h.conn)
	h.cfg.AddSlave(slave)
	return nil
}

func handleWait(h *Handler, userCommand *Command) error {
	if h.cfg.Role() != config.RoleMaster {
		return fmt.Errorf("the %s command is only available for %s servers",
			strings.ToUpper(userCommand.Args[0]),
			strings.ToUpper(config.RoleMaster))
	}

	if len(userCommand.Args) != 3 {
		return fmt.Errorf("the number of arguments for %s is incorrect", userCommand.Args[0])
	}

	numReplicas, err := strconv.Atoi(userCommand.Args[1])
	if err != nil {
		return err
	}

	if numReplicas == 0 {
		h.WriteResponse(encoder.NewInteger(0))
		return nil
	}

	waitTime, err := strconv.Atoi(userCommand.Args[2])
	if err != nil {
		return err
	}

	// h.SetAckSlaves(0)
	h.sendGetAckToSlaves()
	acks := 0

	for {
		select {
		case <-h.acksChan:
			acks++
			if acks >= numReplicas {
				h.WriteResponse(encoder.NewInteger(acks))
				return nil
			}
		case <-time.After(time.Duration(waitTime) * time.Millisecond):
			if acks > 0 {
				h.WriteResponse(encoder.NewInteger(acks))
			} else {
				h.WriteResponse(encoder.NewInteger(len(h.cfg.Slaves())))
			}
			return nil
		}
	}

}

func handleKeys(h *Handler, userCommand *Command) error {
	keys := h.db.GetKeys()
	h.WriteResponse(encoder.NewArray(keys))
	return nil
}
