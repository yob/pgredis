package pgredis

import (
	"github.com/secmask/go-redisproto"
)

type FlushallCommand struct{}

func (cmd *FlushallCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	err := flushAll(redis.db)
	if err == nil {
		return writer.WriteBulkString("OK")
	} else {
		return writer.WriteBulk(nil)
	}
}
