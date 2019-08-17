package pgredis

import (
	"github.com/secmask/go-redisproto"
)

type ttlCommand struct{}

func (cmd *ttlCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	success, resp, err := getString(key, redis.db)
	if success && resp.has_expiry {
		return writer.WriteInt(resp.TTLInSeconds())
	} else if success {
		return writer.WriteInt(-1) // the key exists, but it won't expire
	} else if !success && err == nil {
		return writer.WriteInt(-2) // the key didn't exist
	} else {
		panic(err) // TODO ergh
	}
}
