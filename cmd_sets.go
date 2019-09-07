package pgredis

import (
	"log"

	"github.com/secmask/go-redisproto"
)

type saddCommand struct{}

func (cmd *saddCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	value := command.Get(2)

	updated, err := redis.sets.Add(key, value)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	} else {
		return writer.WriteInt(updated)
	}
}
