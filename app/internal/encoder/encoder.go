package encoder

import "fmt"

const (
	Null     = "$-1\r\n"
	Ok       = "+OK\r\n"
	Pong     = "+PONG\r\n"
	Fullsync = "FULLRESYNC"
)

func NewString(data string) string {
	return fmt.Sprintf("+%s\r\n", data)
}

func NewInteger(number int) string {
	return fmt.Sprintf(":%d\r\n", number)
}

func NewBulkString(data string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(data), data)
}

func NewArray(data []string) string {
	array := fmt.Sprintf("*%d\r\n", len(data))
	for _, d := range data {
		array += NewBulkString(d)
	}

	return array
}

type ListEntry struct {
	EntryId string
	Values  []string
}

func NewList(data []ListEntry) string {
	var list string
	list += fmt.Sprintf("*%d\r\n", len(data))
	for _, v := range data {
		list += "*2\r\n"
		list += NewBulkString(v.EntryId)
		list += NewArray(v.Values)
	}
	return list
}

type ListStream struct {
	StreamId string
	Entries  []ListEntry
}

func NewRead(data []ListStream) string {
	var read string
	read += fmt.Sprintf("*%d\r\n", len(data))
	read += "*2\r\n"
	for _, v := range data {
		read += NewBulkString(v.StreamId)
		read += fmt.Sprintf("*%d\r\n", len(v.Entries))
		for _, w := range v.Entries {
			read += "*2\r\n"
			read += NewBulkString(w.EntryId)
			read += NewArray(w.Values)
		}
	}
	return read
}

func NewError(data string) string {
	return fmt.Sprintf("-ERR %s\r\n", data)
}

func NewRDBFile(fileContent []byte) string {
	return fmt.Sprintf("$%d\r\n%s", len(fileContent), fileContent)
}
