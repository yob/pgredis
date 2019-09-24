package pgredis

import (
	"database/sql"
	"github.com/yob/go-redisproto"
)

type echoCommand struct{}

func (cmd *echoCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	arg := command.Get(1)
	return newPgRedisString(string(arg)), nil
}

type pingCommand struct{}

func (cmd *pingCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	arg := command.Get(1)
	if len(arg) == 0 {
		return newPgRedisString("PONG"), nil
	} else {
		return newPgRedisString(string(arg)), nil
	}
}

type quitCommand struct{}

func (cmd *quitCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	return newPgRedisString("OK"), nil
}
