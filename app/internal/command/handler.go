package command

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/internal/config"
	"github.com/codecrafters-io/redis-starter-go/app/internal/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

const (
	Ping     = "ping"
	Echo     = "echo"
	Set      = "set"
	Get      = "get"
	Info     = "info"
	Replconf = "replconf"
	Psync    = "psync"
	Wait     = "wait"
	Config   = "config"
	Keys     = "keys"
)

const (
	Replication = "replication"
	GetAck      = "getack"
	Ack         = "ack"
	Px          = "px"
	Dir         = "dir"
	DBfilename  = "dbfilename"
)

type Handler struct {
	db           *store.Store
	conn         net.Conn
	cfg          *config.Config
	reader       *bufio.Reader
	writer       *bufio.Writer
	slavesOffset int
	ackSlaves    int
	acksLock     *sync.RWMutex
	acksChan     chan int
}

var commandHandlers = map[string]func(*Handler, *Command) error{
	Ping:     handlePing,
	Echo:     handleEcho,
	Get:      handleGet,
	Set:      handleSet,
	Info:     handleInfo,
	Replconf: handleReplconf,
	Psync:    handlePsync,
	Wait:     handleWait,
	Config:   handleConfig,
	Keys:     handleKeys,
}

func NewHandler(db *store.Store, conn net.Conn, cfg *config.Config, acksChan chan int, locker *sync.RWMutex) *Handler {
	return &Handler{
		db:           db,
		conn:         conn,
		cfg:          cfg,
		reader:       bufio.NewReader(conn),
		writer:       bufio.NewWriter(conn),
		slavesOffset: 0,
		ackSlaves:    0,
		acksLock:     locker,
		acksChan:     acksChan,
	}
}

func (h *Handler) HandleClientConnection() error {
	defer h.conn.Close()

	for {
		userCommand, err := NewCommand(h.reader)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read command, error: %w", err)
		}

		err = h.handleCommand(userCommand)
		if err != nil {
			return fmt.Errorf("error: %w", err)
		}

		h.cfg.UpdateOffset(userCommand.Size)

		h.writer.Flush()
	}
}

func (h *Handler) Handshake() error {
	h.writer.WriteString(encoder.NewArray([]string{"PING"}))
	h.writer.Flush()

	response, err := h.reader.ReadString('\n')
	if err != nil || response != encoder.Pong {
		return fmt.Errorf("incorrect master response")
	}

	h.writer.WriteString(encoder.NewArray([]string{
		Replconf,
		"listening-port",
		strconv.Itoa(h.cfg.Port()),
	}))
	h.writer.Flush()

	response, err = h.reader.ReadString('\n')
	if err != nil || response != encoder.Ok {
		return fmt.Errorf("incorrect master response")
	}

	h.writer.WriteString(encoder.NewArray([]string{
		Replconf,
		"capa",
		"psync2",
	}))
	h.writer.Flush()

	response, err = h.reader.ReadString('\n')
	if err != nil || response != encoder.Ok {
		return fmt.Errorf("incorrect master response")
	}

	h.writer.WriteString(encoder.NewArray([]string{
		Psync,
		"?",
		"-1",
	}))
	h.writer.Flush()

	response, err = h.reader.ReadString('\n')
	responseCommand := strings.Split(response, " ")[0]
	if err != nil || responseCommand != "+"+encoder.Fullsync {
		return fmt.Errorf("incorrect master response")
	}

	// Skip the RDB file sent by the master during the handshake phase.
	_, err = h.reader.ReadBytes(162)
	if err != nil {
		return err
	}

	return nil
}

// Handles responses to commands sent to the master server.
// Only slaves send responses to `REPLCONF GETACK` commands.
// This function writes the response to the client connection
// if the server is a master.
func (h *Handler) WriteResponse(msg string) {
	if h.cfg.Role() == config.RoleMaster {
		h.writer.WriteString(msg)
	}
}

func (h *Handler) UpdateSlavesOffset(commandBytes int) {
	h.slavesOffset += commandBytes
}

// func (h *Handler) AckSlaves() int {
// 	h.acksLock.RLock()
// 	defer h.acksLock.RUnlock()
// 	return h.ackSlaves
// }

func (h *Handler) NotifyAckSlaves() {
	h.acksChan <- 1
}

// func (h *Handler) IncrementAckSlaves() {
// 	h.acksLock.Lock()
// 	h.ackSlaves++
// 	h.acksLock.Unlock()
// 	h.acksChan <- h.ackSlaves
// }

// func (h *Handler) SetAckSlaves(val int) {
// 	h.acksLock.Lock()
// 	h.ackSlaves = val
// 	h.acksLock.Unlock()
// }

func (h *Handler) handleCommand(userCommand *Command) error {
	instruction := strings.ToLower(userCommand.Args[0])
	handler, exist := commandHandlers[instruction]
	if !exist {
		return fmt.Errorf("unknown command: %s", strings.ToUpper(instruction))
	}
	return handler(h, userCommand)
}

func (h *Handler) sendGetAckToSlaves() {
	wg := &sync.WaitGroup{}
	command := encoder.NewArray([]string{"REPLCONF", "GETACK", "*"})
	for _, slave := range h.cfg.Slaves() {
		wg.Add(1)
		go slave.PropagateCommand(command, wg)
	}
	wg.Wait()
	h.UpdateSlavesOffset(len([]byte(command)))
}
