package pgredis

import (
	"database/sql"
	"github.com/secmask/go-redisproto"
)

type flushallCommand struct{}

func (cmd *flushallCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	err := redis.keys.FlushAll(tx)
	if err == nil {
		return newPgRedisString("OK"), nil
	} else {
		return nil, err
	}
}
