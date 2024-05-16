package rdb

const (
	MAGIC_NUMBER           = "REDIS"
	DATABASE_SELECT_OPCODE = 0xFE
	END_OPCODE             = 0xFF
	OPCODE_EXPIRETIME_MS   = 0xFC
	OPCODE_EXPIRETIME      = 0xFD
	OPCODE_SELECTDB        = 0xFE
	OPCODE_RESIZEDB        = 0xFB
)

// Length Encoding Constants
const (
	// 00
	ENC_INT8 = 0b00
	// 01
	ENC_INT16 = 0b01
	// 10
	ENC_INT32 = 0b10
	// 11
	ENC_LZF = 0b11
)
