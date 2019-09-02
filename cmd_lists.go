package pgredis

import (
	"github.com/secmask/go-redisproto"
)

type llenCommand struct{}

func (cmd *llenCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	length, err := redis.lists.Length(key)
	if err != nil {
		return writer.WriteBulk(nil)
	}
	return writer.WriteInt(int64(length))
}
