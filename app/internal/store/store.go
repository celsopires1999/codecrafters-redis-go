package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/rdb"
)

type Store struct {
	StringType
	StreamType
}

type StringType struct {
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
		StringType{
			kv: make(map[string]StoreItem),
		},
		StreamType{
			stream: make(map[StreamId]map[EntryId][]Fact),
		},
	}
}

func (s *StringType) Set(k, v string, expires bool, intTime int64) {
	var expireAt time.Time
	if expires {
		expireAt = time.Now().Add(time.Duration(intTime) * time.Millisecond)
	}

	s.save(k, v, expires, expireAt)
}

func (s *StringType) Load(k, v string, expires bool, xp int64) {
	var expireAt time.Time
	if expires {
		expireAt = time.UnixMilli(xp)
	}

	s.save(k, v, expires, expireAt)
}

func (s *StringType) save(k, v string, expires bool, expireAt time.Time) {
	s.mu.Lock()
	s.kv[k] = StoreItem{
		value:    v,
		expires:  expires,
		expireAt: expireAt,
	}
	s.mu.Unlock()
}

func (s *StringType) Get(k string) (string, error) {
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

func (s *StringType) DeleteExpiredItems() {
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

func (s *StringType) DeleteItems(keys []string) {
	s.mu.Lock()
	for _, key := range keys {
		delete(s.kv, key)
	}
	s.mu.Unlock()
}

func (s *StringType) GetKeys() []string {
	keys := make([]string, 0, len(s.kv))
	for k := range s.kv {
		keys = append(keys, k)
	}

	return keys
}

func (s *StringType) ReadRDBFile(path string) error {
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

	return err
}

func (s *StringType) loadFileContent(reader *bufio.Reader) error {
	opcode, err := reader.ReadByte()
	if err != nil {
		return err
	}
	// End of the RDB file
	if opcode == rdb.END_OPCODE {
		return nil
	}

	for {
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
		}

		// Exp time byte
		opcode, err = reader.ReadByte()
		if err != nil {
			return err
		}

		// End of the RDB file
		if opcode == rdb.END_OPCODE {
			return nil
		}

		expires := false
		xp := int64(0)

		if opcode == rdb.OPCODE_EXPIRETIME_MS {
			expires = true
			ex := make([]byte, 9)
			_, err = reader.Read(ex)
			if err != nil {
				return err
			}
			xp = int64(binary.LittleEndian.Uint64(ex))
		}
		if opcode == rdb.OPCODE_EXPIRETIME {
			expires = true
			ex := make([]byte, 5)
			_, err = reader.Read(ex)
			if err != nil {
				return err
			}
			xp = int64(binary.LittleEndian.Uint64(ex)) * 1000
		}

		// Key Encoding
		keyLength, err := rdb.LengthEncodedInt(reader)
		if err != nil {
			return err
		}
		keyBytes := make([]byte, keyLength)
		_, err = reader.Read(keyBytes)
		if err != nil {
			return err
		}

		// Value Encoding
		valueLength, err := rdb.LengthEncodedInt(reader)
		if err != nil {
			return err
		}
		valueBytes := make([]byte, valueLength)
		_, err = reader.Read(valueBytes)
		if err != nil {
			return err
		}

		s.Load(string(keyBytes), string(valueBytes), expires, xp)
	}
}
