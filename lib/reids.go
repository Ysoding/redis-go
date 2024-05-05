package lib

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/Ysoding/redis-go/lib/core"
	"github.com/Ysoding/redis-go/lib/parser"
	"github.com/golang/glog"
)

type RedisServer struct {
	DB *core.DB
}

var Server = &RedisServer{DB: &core.DB{Dict: make(map[string]string)}}

func Start() {
	flag.Parse()
	defer glog.Flush()

	glog.Info("Start")

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

func handleClient(conn net.Conn) {
	glog.Info("Started Connection(%s)\n", conn.RemoteAddr().String())
	defer conn.Close()

	rp := parser.NewRespParser(conn)
	for {
		reqData, err := rp.Parse()
		if err != nil {
			if err == io.EOF {
				break
			}
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
		msgData, _ := data[1].(string)
		replyMsg = fmt.Sprintf("$%d\r\n%s\r\n", len(msgData), msgData)
	case "ping":
		replyMsg = "$4\r\nPONG\r\n"
	case "set":
		key, _ := data[1].(string)
		val, _ := data[2].(string)

		Server.DB.Dict[key] = val
		replyMsg = "+OK\r\n"
	case "get":
		key, _ := data[1].(string)
		val, exist := Server.DB.Dict[key]
		if !exist {
			replyMsg = "$5\r\n(nil)\r\n"
		} else {
			replyMsg = fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
		}
	case "command":
		replyMsg = "+OK\r\n"
	default:
		replyMsg = "+OK\r\n"
	}

	conn.Write([]byte(replyMsg))
	return nil
}
