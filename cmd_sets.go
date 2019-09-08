package pgredis

import (
	"log"

	"github.com/secmask/go-redisproto"
)

type saddCommand struct{}

func (cmd *saddCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	values := make([][]byte, 0)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}

	updated, err := redis.sets.Add(key, values)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	} else {
		return writer.WriteInt(updated)
	}
}

type smembersCommand struct{}

func (cmd *smembersCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)

	values, err := redis.sets.Members(key)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	} else {
		return writer.WriteBulkStrings(values)
	}
}
