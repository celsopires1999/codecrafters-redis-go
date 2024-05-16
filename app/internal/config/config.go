package config

import "fmt"

type Config struct {
	port        int
	role        string
	replicaOf   string
	replID      string
	replOffset  int
	slaves      []*Slave
	dir         string
	rdbFileName string
}
type Option func(c *Config)

const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

func NewConfig(options ...Option) *Config {
	config := &Config{
		port:        6379,
		role:        "master",
		replID:      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replOffset:  0,
		slaves:      []*Slave{},
		dir:         "/tmp/redis-files",
		rdbFileName: "db.rdb",
	}
	for _, opt := range options {
		opt(config)
	}
	return config
}

func (c *Config) Port() int {
	return c.port
}

func (c *Config) Role() string {
	return c.role
}

func (c *Config) ReplID() string {
	return c.replID
}

func (c *Config) ReplOffset() int {
	return c.replOffset
}

func (c *Config) ReplicaOf() string {
	return c.replicaOf
}

func (c *Config) Dir() string {
	return c.dir
}

func (c *Config) RDBFileName() string {
	return c.rdbFileName
}

func (c *Config) Slaves() []*Slave {
	return c.slaves
}

func (c *Config) AddSlave(slave *Slave) {
	c.slaves = append(c.slaves, slave)
}

func (c *Config) UpdateOffset(bytes int) {
	c.replOffset += bytes
}

func (c *Config) RDBFilePath() string {
	return fmt.Sprintf("%s/%s", c.dir, c.rdbFileName)
}

func WithPort(port int) Option {
	return func(c *Config) {
		c.port = port
	}
}

func WithSlave(master string) Option {
	return func(c *Config) {
		c.role = "slave"
		c.replicaOf = master
	}
}

func WithDir(dir string) Option {
	return func(c *Config) {
		c.dir = dir
	}
}

func WithRDBFileName(fileName string) Option {
	return func(c *Config) {
		c.rdbFileName = fileName
	}
}
