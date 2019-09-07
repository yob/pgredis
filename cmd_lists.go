package pgredis

import (
	"log"
	"strconv"

	"github.com/secmask/go-redisproto"
)

type llenCommand struct{}

func (cmd *llenCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	length, err := redis.lists.Length(key)
	if err != nil {
		return writer.WriteBulk(nil)
	}
	return writer.WriteInt(int64(length))
}

type lpushCommand struct{}

func (cmd *lpushCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	values := make([][]byte, 0)
	key := command.Get(1)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}
	newLength, err := redis.lists.LeftPush(key, values)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
	return writer.WriteInt(int64(newLength))
}

type lrangeCommand struct{}

func (cmd *lrangeCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	start, _ := strconv.Atoi(string(command.Get(2)))
	end, _ := strconv.Atoi(string(command.Get(3)))
	items, err := redis.lists.Lrange(key, start, end)
	if err == nil {
		return writer.WriteBulkStrings(items)
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

type rpushCommand struct{}

func (cmd *rpushCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	values := make([][]byte, 0)
	key := command.Get(1)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}
	newLength, err := redis.lists.RightPush(key, values)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
	return writer.WriteInt(int64(newLength))
}
