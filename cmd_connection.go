package pgredis

import (
	"github.com/secmask/go-redisproto"
)

type echoCommand struct{}

func (cmd *echoCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	arg := command.Get(1)
	return writer.WriteBulkString(string(arg))
}

type pingCommand struct{}

func (cmd *pingCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	arg := command.Get(1)
	if len(arg) == 0 {
		return writer.WriteBulkString("PONG")
	} else {
		return writer.WriteBulkString(string(arg))
	}
}
