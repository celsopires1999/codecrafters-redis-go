package command

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

const (
	SimpleString = '+'
	BulkString   = '$'
	Arrays       = '*'
)

type Command struct {
	Args []string
	Size int
}

// NewCommand creates a new Command object by reading input from the given bufio.Reader.
//
// It reads a line of input from the reader and trims any leading or trailing whitespace.
// Then, it determines the type of the command based on the first character of the line.
// If the command is a simple string, it assigns the remaining characters as the only argument.
// If the command is a bulk string, it parses the bulk string and adds it as the only argument.
// If the command is an array, it parses the array and assigns the arguments and total size.
//
// The function returns a pointer to the created Command object and any error encountered.
func NewCommand(reader *bufio.Reader) (*Command, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	command := &Command{
		Size: len([]byte(line)),
	}
	line = strings.TrimSpace(line)

	switch line[0] {
	default:
		command.Args = []string{}

	case SimpleString:
		command.Args = []string{line[1:]}

	case BulkString:
		formattedString, bytes, err := parseBulkString(reader)
		if err != nil {
			return nil, err
		}
		command.Args = []string{formattedString}
		command.Size += bytes

	case Arrays:
		args, bytes, err := parseArray(reader, line)
		if err != nil {
			return nil, err
		}
		command.Args = args
		command.Size += bytes
	}

	return command, nil
}

// parseBulkString parses a Redis bulk string from the given bufio.Reader.
//
// The function returns the parsed string, the number of bytes read from
// the reader, and any error encountered.
func parseBulkString(reader *bufio.Reader) (string, int, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", 0, err
	}

	bytes := len([]byte(line))
	line = strings.TrimSpace(line)

	size := 0
	fmt.Sscanf(line, "$%d", &size)
	data := make([]byte, size+2)

	commandBytes, err := io.ReadFull(reader, data)
	bytes += commandBytes
	if err != nil {
		return "", 0, err
	}

	return string(data[:size]), bytes, nil
}

// parseArray parses a Redis array from the given bufio.Reader.
//
// The function returns the parsed array, the number of bytes read from
// the reader, and any error encountered.
func parseArray(reader *bufio.Reader, line string) ([]string, int, error) {
	size := 0
	_, err := fmt.Sscanf(line, "*%d", &size)
	if err != nil {
		return nil, 0, err
	}

	arrayArgs := make([]string, size)
	totalBytes := 0
	for i := 0; i < size; i++ {
		arg, bytes, err := parseBulkString(reader)
		if err != nil {
			return nil, 0, err
		}
		totalBytes += bytes
		arrayArgs[i] = arg
	}

	return arrayArgs, totalBytes, nil
}
