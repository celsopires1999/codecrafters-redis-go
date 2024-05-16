package store

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/rdb"
)

type Store struct {
	kv map[string]StoreItem
	mu sync.Mutex
}

type StoreItem struct {
	value    string
	expires  bool
	expireAt time.Time
}

func NewStore() *Store {
	return &Store{
		kv: make(map[string]StoreItem),
	}
}

func (s *Store) Set(k, v string, expires bool, intTime int64) {
	var expireAt time.Time
	if expires {
		expireAt = time.Now().Add(time.Duration(intTime) * time.Millisecond)
	}

	s.mu.Lock()
	s.kv[k] = StoreItem{
		value:    v,
		expires:  expires,
		expireAt: expireAt,
	}
	s.mu.Unlock()
}

func (s *Store) Get(k string) (string, error) {
	s.mu.Lock()
	item, ok := s.kv[k]
	s.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("%s not found", k)
	}

	if item.expires && item.expireAt.Before(time.Now()) {
		return "", fmt.Errorf("%s expired", k)
	}

	return item.value, nil
}

func (s *Store) DeleteExpiredItems() {
	for {
		time.Sleep(100 * time.Millisecond)
		keys := make([]string, 0)
		for k, v := range s.kv {
			if v.expires && v.expireAt.Before(time.Now()) {
				keys = append(keys, k)
			}
		}
		s.DeleteItems(keys)
	}
}

func (s *Store) DeleteItems(keys []string) {
	s.mu.Lock()
	for _, key := range keys {
		delete(s.kv, key)
	}
	s.mu.Unlock()
}

func (s *Store) GetKeys() []string {
	keys := make([]string, 0, len(s.kv))
	for k := range s.kv {
		keys = append(keys, k)
	}

	return keys
}

func (s *Store) ReadRDBFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	err = rdb.CheckMagicNumber(reader)
	if err != nil {
		return err
	}

	err = rdb.SkipMetadata(reader)
	if err != nil {
		return err
	}

	// Read db number
	// FE 00                       # Indicates database selector. db number = 00
	_, err = reader.ReadByte()
	if err != nil {
		return err
	}

	err = s.loadFileContent(reader)
	if err == io.EOF || err == nil {
		return nil
	}
	// if err != nil {
	// 	return err
	// }
	return err
}

func (s *Store) loadFileContent(reader *bufio.Reader) error {
	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			return err
		}

		// End of the RDB file
		if opcode == rdb.END_OPCODE {
			return nil
		}

		if opcode == rdb.OPCODE_SELECTDB {
			err = rdb.ReadSelectDB(reader)
			if err != nil {
				return err
			}
		}

		if opcode == rdb.OPCODE_RESIZEDB {
			err = rdb.ReadResizeDB(reader)
			if err != nil {
				return err
			}
			reader.Discard(1) // adjust for
		}

		// Skipping exp time bytes
		if opcode == rdb.OPCODE_EXPIRETIME_MS {
			fmt.Println("SKIPPING MS EXP TIME")
			reader.Discard(9)
		}
		if opcode == rdb.OPCODE_EXPIRETIME {
			fmt.Println("SKIPPING NOT MS EXP TIME")
			reader.Discard(5)
		}

		// Length Encoding
		keyLength, err := rdb.LengthEncodedInt(reader)
		if err != nil {
			return err
		}
		keyBytes := make([]byte, keyLength)
		_, err = reader.Read(keyBytes)
		if err != nil {
			return err
		}

		// Length Encoding
		valueLength, err := rdb.LengthEncodedInt(reader)
		if err != nil {
			return err
		}
		valueBytes := make([]byte, valueLength)
		_, err = reader.Read(valueBytes)
		if err != nil {
			return err
		}

		// TODO: add support por exp time latter
		fmt.Printf("READ KEY: %s, READ VAL: %s\n", string(keyBytes), string(valueBytes))
		s.Set(string(keyBytes), string(valueBytes), false, 0)
	}
}
