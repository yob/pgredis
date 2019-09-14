package pgredis

import (
	"github.com/secmask/go-redisproto"
)

type echoCommand struct{}

func (cmd *echoCommand) Execute(command *redisproto.Command, redis *PgRedis) pgRedisValue {
	arg := command.Get(1)
	return newPgRedisString(string(arg))
}

type pingCommand struct{}

func (cmd *pingCommand) Execute(command *redisproto.Command, redis *PgRedis) pgRedisValue {
	arg := command.Get(1)
	if len(arg) == 0 {
		return newPgRedisString("PONG")
	} else {
		return newPgRedisString(string(arg))
	}
}

type quitCommand struct{}

func (cmd *quitCommand) Execute(command *redisproto.Command, redis *PgRedis) pgRedisValue {
	return newPgRedisString("OK")
}
