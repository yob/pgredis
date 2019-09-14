package pgredis

import (
	"fmt"

	"database/sql"
	"github.com/secmask/go-redisproto"
)

type redisCommand interface {
	Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue
}

type unrecognisedCommand struct{}

func (cmd *unrecognisedCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	return newPgRedisError(fmt.Sprintf("Command %s not recognised", command.Get(0)))
}
