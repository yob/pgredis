package pgredis

import (
	"github.com/secmask/go-redisproto"
)

type flushallCommand struct{}

func (cmd *flushallCommand) Execute(command redisProtoCommand, redis *PgRedis, writer *redisproto.Writer) error {
	err := redis.flushAll()
	if err == nil {
		return writer.WriteBulkString("OK")
	} else {
		return writer.WriteBulk(nil)
	}
}
