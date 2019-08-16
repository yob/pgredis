package pgredis

import (
	"log"
	"strconv"

	"github.com/secmask/go-redisproto"
)

type getCommand struct{}

func (cmd *getCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	resp, err := getString(command.Get(1), redis.db)
	if resp != nil {
		return writer.WriteBulkString(string(resp))
	} else if resp == nil && err == nil {
		return writer.WriteBulk(nil)
	} else {
		panic(err) // TODO ergh
	}
}

type getrangeCommand struct{}

func (cmd *getrangeCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	data, err := getString(command.Get(1), redis.db)
	if data != nil {
		start, _ := strconv.Atoi(string(command.Get(2)))
		end, _ := strconv.Atoi(string(command.Get(3)))

		if start < 0 {
			start = len(data) + start
		}

		if end < 0 {
			end = len(data) + end
		}

		end += 1

		if end < start {
			end = start
		}

		if start > len(data) {
			start = len(data)
		}

		if end > len(data) {
			end = len(data)
		}

		return writer.WriteBulkString(string(data[start:end]))
	} else if data == nil && err == nil {
		return writer.WriteBulk(nil)
	} else {
		return err
	}
}

type setCommand struct{}

func (cmd *setCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	expiry_millis := 0

	flag := string(command.Get(3))
	if flag == "EX" {
		expiry_secs, _ := strconv.Atoi(string(command.Get(4)))
		expiry_millis = expiry_secs * 1000
	}

	err := setString(command.Get(1), command.Get(2), expiry_millis, redis.db)
	if err == nil {
		return writer.WriteBulkString("OK")
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}
