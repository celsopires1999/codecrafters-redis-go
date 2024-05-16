package rdb

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"os"
)

const (
	rdbFile        = "rdb/data.txt"
	rdbExtension   = ".rdb"
	maxBytesToRead = 1024
)

// Function to pass `Empty RDB Transfer` stage probably can be removed latter
func GetEmptyRDBContent() ([]byte, error) {
	content, err := os.ReadFile(rdbFile)
	if err != nil {
		return nil, err
	}

	result := make([]byte, hex.DecodedLen(len(content)))
	_, err = hex.Decode(result, content)
	if err != nil {
		return nil, err
	}

	return result, nil
}

/*
The file header consists of two parts: the Magic Number and the version number
- RDB files start with the ASCII-encoded 'REDIS' as the File Magic Number to represent their file type
- The next 4 bytes represent the version number of the RDB file
*/
func CheckMagicNumber(reader *bufio.Reader) error {
	// 52 45 44 49 53              # Magic String "REDIS"
	magicNumber, err := reader.Peek(5)
	if err != nil {
		return err
	}
	if string(magicNumber) != MAGIC_NUMBER {
		return fmt.Errorf("invalid RDB file")
	}

	_, err = reader.Discard(5)
	if err != nil {
		return err
	}

	// The next 4 bytes are the RDB Version Number
	// 30 30 30 33                 # RDB Version Number as ASCII string. "0003" = 3
	_, err = reader.Discard(4)
	if err != nil {
		return err
	}
	return nil
}

func SkipMetadata(reader *bufio.Reader) error {
	currentBytesRead := 0
	// skip to 0xFE opcode
	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			return err
		}
		currentBytesRead++

		if opcode == DATABASE_SELECT_OPCODE {
			return nil
		}
		if currentBytesRead >= maxBytesToRead {
			return fmt.Errorf("invalid RDB file format")
		}
		if opcode == END_OPCODE {
			return fmt.Errorf("invalid RDB file format")
		}
	}
}

func ReadSelectDB(reader *bufio.Reader) error {
	// FE <database-id>             # Select the database to associate the following keys with.
	_, err := reader.ReadByte()
	if err != nil {
		return err
	}

	_, err = reader.ReadByte()
	if err != nil {
		return err
	}

	return nil
}

func ReadResizeDB(reader *bufio.Reader) error {
	// FB <length> <length>         # Resize database
	// Number of keys in the database (Length Encoding)
	numOfKeys, err := LengthEncodedInt(reader)
	if err != nil {
		return err
	}
	log.Println("Number of Keys in the Database: ", numOfKeys)
	// Number of keys with an expire time set
	numOfKeysWithExpireTime, err := LengthEncodedInt(reader)
	if err != nil {
		return err
	}
	log.Println("Number of Keys with an Expire Time Set: ", numOfKeysWithExpireTime)
	return nil
}

/*
Bits	How to parse
00	The next 6 bits represent the length
01	Read one additional byte. The combined 14 bits represent the length
10	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
11	The next object is encoded in a special format. The remaining 6 bits indicate the format. May be used to store numbers or Strings, see String Encoding
*/
func LengthEncodedInt(reader *bufio.Reader) (int, error) {
	opcode, err := reader.ReadByte()
	if err != nil {
		return -1, err
	}
	// Represented the opcode in little endian -- 2 most significant bits
	switch opcode >> 6 {
	case ENC_INT8:
		// It's 00, so read the next 6 bits
		return int(binary.LittleEndian.Uint16([]byte{opcode, 00})), nil
	case ENC_INT16:
		// It's 01, so read one additional byte
		int16Byte, err := reader.ReadByte()
		if err != nil {
			return -1, err
		}
		return int(binary.LittleEndian.Uint16([]byte{opcode & 0x3F, int16Byte})), nil
	case ENC_INT32:
		// It's 10, so discard the remaining 6 bits
		int32Bytes, err := reader.Peek(4)
		if err != nil {
			return -1, err
		}
		_, err = reader.Discard(4)
		if err != nil {
			return -1, err
		}
		return int(binary.LittleEndian.Uint32([]byte{opcode & 0x3F, int32Bytes[0], int32Bytes[1], int32Bytes[2], int32Bytes[3]})), nil
	case ENC_LZF:
		// It's 11, so the next object is encoded in a special format
		// The remaining 6 bits indicate the format
		switch opcode & 0x3F {
		case 0:
			buf := make([]byte, 1)
			_, err := reader.Read(buf)
			if err != nil {
				return -1, err
			}
			return int(binary.LittleEndian.Uint16([]byte{buf[0], 00})), nil
		case 1:
			buf := make([]byte, 2)
			_, err := reader.Read(buf)
			if err != nil {
				return -1, err
			}
			return int(binary.LittleEndian.Uint16(buf)), nil
		case 2:
			buf := make([]byte, 4)
			_, err := reader.Read(buf)
			if err != nil {
				return -1, err
			}
			return int(binary.LittleEndian.Uint32(buf)), nil
		}
	}
	return -1, nil
}
