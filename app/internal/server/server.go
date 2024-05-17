package server

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/internal/command"
	"github.com/codecrafters-io/redis-starter-go/app/internal/config"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

const (
	DEFAULT_HOST = "0.0.0.0"
)

type Server struct {
	cfg    *config.Config
	db     *store.Store
	stream *store.Stream
}

func NewServer(cfg *config.Config, db *store.Store, stream *store.Stream) *Server {
	return &Server{
		cfg:    cfg,
		db:     db,
		stream: stream,
	}
}

func (s *Server) Run() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", DEFAULT_HOST, s.cfg.Port()))
	if err != nil {
		return fmt.Errorf("failed to bind to port %d", s.cfg.Port())
	}
	// teardown the server when the program exists
	defer listener.Close()
	log.Println("server listenning at", s.cfg.Port())

	// clean expired items
	go s.db.DeleteExpiredItems()

	acksChan := make(chan struct{}, 10)
	locker := &sync.RWMutex{}

	for {
		// block until we receive an incoming connection
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			continue
		}
		// handle client connection
		connHandler := command.NewHandler(s.db, s.stream, conn, s.cfg, acksChan, locker)

		go s.serveConnection(connHandler)
	}
}

func (s *Server) Handshake() error {
	conn, err := net.Dial("tcp", s.cfg.ReplicaOf())
	if err != nil {
		return fmt.Errorf("failed to dial with master error: %w", err)
	}

	acksChan := make(chan struct{}, 10)
	connHandler := command.NewHandler(s.db, s.stream, conn, s.cfg, acksChan, &sync.RWMutex{})
	if err := connHandler.Handshake(); err != nil {
		return fmt.Errorf("failed to handshake, error: %w", err)
	}

	go s.serveConnection(connHandler)

	return nil
}

func (s *Server) serveConnection(connHandler *command.Handler) {
	err := connHandler.HandleClientConnection()
	if err != nil {
		log.Printf("something happened with the client connection, err: %s\n", err.Error())
	}
}
