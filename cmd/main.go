package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"strings"

	"github.com/Ysoding/redis-go/parser"
	"github.com/golang/glog"
)

func handleClient(conn net.Conn) {
	fmt.Printf("Started Connection(%s)\n", conn.RemoteAddr().String())
	defer conn.Close()

	rp := parser.NewRespParser(conn)
	for {
		reqData, err := rp.Parse()
		if err != nil {
			glog.Error(err)
			continue
		}

		if reqData == nil {
			glog.Info("empty parse request data")
			continue
		}
		parsedData, ok := reqData.([]interface{})
		if !ok {
			glog.Error("require multiple bulk protocol")
			continue
		}
		err = executeCommand(parsedData, conn)
		if err != nil {
			glog.Error("executeCommand error: ", err.Error())
		}
	}

}

func executeCommand(data []interface{}, conn net.Conn) error {
	if len(data) == 0 {
		return nil
	}

	commandName, ok := data[0].(string)
	if !ok {
		return errors.New("command name is not a string")
	}

	commandName = strings.ToLower(commandName)

	replyMsg := ""

	switch commandName {
	case "echo":
		if len(data) != 2 {
			return errors.New("echo syntax error")
		}

		msgData, ok := data[1].(string)
		if !ok {
			return errors.New("msg data is not string")
		}
		replyMsg = fmt.Sprintf("$%d\r\n%s\r\n", len(msgData), msgData)

	case "ping":
		replyMsg = "$4\r\nPONG\r\n"
	}

	conn.Write([]byte(replyMsg))
	return nil
}

func main() {
	flag.Parse()
	defer glog.Flush()

	glog.Info("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		glog.Fatalln("Failed to bind to port 6379")
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			glog.Error("Error accepting connection: ", err.Error())
			continue
		}

		go handleClient(conn)
	}

}
