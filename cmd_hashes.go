package pgredis

import (
	"log"

	"github.com/secmask/go-redisproto"
)

type hsetCommand struct{}

func (cmd *hsetCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	field := command.Get(2)
	value := command.Get(3)
	inserted, err := redis.hashes.Set(key, field, value)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	} else {
		return writer.WriteInt(inserted)
	}
}
