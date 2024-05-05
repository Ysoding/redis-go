package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
)

type RespParser struct {
	reader *bufio.Reader
}

func NewRespParser(reader io.Reader) RespParser {
	return RespParser{
		reader: bufio.NewReader(reader),
	}
}

func (p *RespParser) Parse() (interface{}, error) {
	line, err := p.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	length := len(line)
	if length <= 2 || line[length-2] != '\r' {
		return nil, nil
	}

	line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
	switch line[0] {
	case '*':
		return p.parseArray(line)
	case '$':
		return p.parseBulkString(line)
	default:
		return nil, nil
	}
}

func (p *RespParser) parseBulkString(header []byte) (string, error) {
	strLen, err := strconv.ParseUint(string(header[1:]), 10, 64)
	if err != nil {
		return "", protocolError("illegal bulk string header")
	}

	data, err := p.reader.ReadBytes('\n')
	data = bytes.TrimSuffix(data, []byte{'\r', '\n'})

	if err != nil || len(data) != int(strLen) {
		return "", protocolError("illegal bulk data")
	}

	return string(data), nil
}

func (p *RespParser) parseArray(header []byte) (interface{}, error) {
	nStrs, err := strconv.ParseUint(string(header[1:]), 10, 64)
	if err != nil {
		return nil, protocolError("illegal array header")
	}

	arr := make([]interface{}, nStrs)
	for i := uint64(0); i < nStrs; i++ {
		arr[i], err = p.Parse()
		if err != nil {
			return nil, err
		}
	}

	return arr, nil
}

func protocolError(msg string) error {
	return errors.New("protocol error: " + msg)
}
