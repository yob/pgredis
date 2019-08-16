package pgredis

import (
	"fmt"

	"github.com/secmask/go-redisproto"
)

type redisCommand interface {
	Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error
}

type UnrecognisedCommand struct{}

func (cmd *UnrecognisedCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	return writer.WriteError(fmt.Sprintf("Command %s not recognised", command.Get(0)))
}
