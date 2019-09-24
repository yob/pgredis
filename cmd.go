package pgredis

import (
	"errors"
	"fmt"

	"database/sql"
	"github.com/yob/go-redisproto"
)

type redisCommand interface {
	Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error)
}

type unrecognisedCommand struct{}

func (cmd *unrecognisedCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	return nil, errors.New(fmt.Sprintf("Command %s not recognised", command.Get(0)))
}
