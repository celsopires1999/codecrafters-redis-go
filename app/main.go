package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/internal/config"
	"github.com/codecrafters-io/redis-starter-go/app/internal/server"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

func main() {
	options := setServerOptions()
	cfg := config.NewConfig(options...)
	db := store.NewStore()
	stream := store.NewStream()

	log.Println("searching for rdb file to load data...")
	err := db.ReadRDBFile(cfg.RDBFilePath())
	if err != nil {
		log.Printf("Error: %s\n failed to read rdb file, starting the server with empty data...\n", err.Error())
	}

	server := server.NewServer(cfg, db, stream)

	if cfg.Role() == "slave" {
		if err := server.Handshake(); err != nil {
			log.Printf("failed handshake to %s, error: %s", cfg.ReplicaOf(), err.Error())
			os.Exit(1)
		}
	}

	if err := server.Run(); err != nil {
		log.Println("failed to start the server:\n", err.Error())
		os.Exit(1)
	}
}

// Looks for cmdOptions passed by the user to start the server
func setServerOptions() []config.Option {
	var options []config.Option
	var port int
	var replicaOfHost string
	var dir string
	var dbfilename string

	flag.IntVar(&port, "port", 0, "server port")
	flag.StringVar(&replicaOfHost, "replicaof", "", "replica of")
	flag.StringVar(&dir, "dir", "", "data directory")
	flag.StringVar(&dbfilename, "dbfilename", "", "database filename")

	flag.Parse()

	if port != 0 {
		options = append(options, config.WithPort(port))
	}

	if dir != "" {
		options = append(options, config.WithDir(dir))
	}
	if dbfilename != "" {
		options = append(options, config.WithRDBFileName(dbfilename))
	}

	if replicaOfHost != "" {
		replicaOfPort := 0
		if len(flag.Args()) == 0 {
			replicaOfHost, replicaOfPort = parseReplicaOf(replicaOfHost)
		} else {
			replicaOfPort = parseReplicaOfPort(flag.Args()[0])
		}

		master := fmt.Sprintf("%s:%d", replicaOfHost, replicaOfPort)
		options = append(options, config.WithSlave(master))
	}

	return options
}

func parseReplicaOf(s string) (string, int) {
	replicaOfHost := ""
	replicaOfPort := 0

	_, err := fmt.Sscanf(s, "%s %d", &replicaOfHost, &replicaOfPort)
	if err != nil {
		log.Fatalf("error parsing replicaof %s: %s", s, err.Error())
	}

	return replicaOfHost, replicaOfPort
}

func parseReplicaOfPort(s string) int {
	replicaOfPort, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("error parsing port of replica: %s\n", err)
	}
	return replicaOfPort
}
