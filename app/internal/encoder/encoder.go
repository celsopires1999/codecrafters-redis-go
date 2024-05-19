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

type ListItem struct {
	Id     string
	Values []string
}

func NewList(data []ListItem) string {
	var list string
	list += fmt.Sprintf("*%d\r\n", len(data))
	for _, v := range data {
		list += "*2\r\n"
		list += NewBulkString(v.Id)
		list += NewArray(v.Values)
	}
	return list
}

func NewError(data string) string {
	return fmt.Sprintf("-ERR %s\r\n", data)
}

func NewRDBFile(fileContent []byte) string {
	return fmt.Sprintf("$%d\r\n%s", len(fileContent), fileContent)
}
