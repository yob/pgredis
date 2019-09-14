package pgredis

import (
	"database/sql"
	"github.com/secmask/go-redisproto"
)

type flushallCommand struct{}

func (cmd *flushallCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	err := redis.flushAll()
	if err == nil {
		return newPgRedisString("OK")
	} else {
		return newPgRedisNil()
	}
}
