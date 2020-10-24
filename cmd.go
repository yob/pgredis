package pgredis

import (
	"errors"
	"fmt"

	"database/sql"
)

type redisCommand interface {
	Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error)
}

type unrecognisedCommand struct{}

func (cmd *unrecognisedCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	return nil, errors.New(fmt.Sprintf("Command %s not recognised", command.Get(0)))
}
