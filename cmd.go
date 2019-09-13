package pgredis

import (
	"fmt"

	"github.com/secmask/go-redisproto"
)

type redisCommand interface {
	Execute(command redisProtoCommand, redis *PgRedis, writer *redisproto.Writer) error
}

type unrecognisedCommand struct{}

func (cmd *unrecognisedCommand) Execute(command redisProtoCommand, redis *PgRedis, writer *redisproto.Writer) error {
	return writer.WriteError(fmt.Sprintf("Command %s not recognised", command.Get(0)))
}
