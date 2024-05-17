package config

import (
	"bufio"
	"net"
	"sync"
)

type Slave struct {
	conn net.Conn
	mu   sync.Mutex
}

func NewSlave(conn net.Conn) *Slave {
	return &Slave{
		conn: conn,
	}
}

func (s *Slave) PropagateCommand(command string, wg *sync.WaitGroup) {
	defer wg.Done()

	s.mu.Lock()
	writer := bufio.NewWriter(s.conn)
	writer.WriteString(command)
	writer.Flush()
	s.mu.Unlock()
}
