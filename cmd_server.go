package pgredis

import (
	"database/sql"
	"github.com/secmask/go-redisproto"
)

type flushallCommand struct{}

func (cmd *flushallCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx, writer *redisproto.Writer) error {
	err := redis.flushAll()
	if err == nil {
		return writer.WriteBulkString("OK")
	} else {
		return writer.WriteBulk(nil)
	}
}
