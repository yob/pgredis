package pgredis

import (
	"log"
	"strconv"

	"github.com/secmask/go-redisproto"
)

type expireCommand struct{}

func (cmd *expireCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	seconds, _ := strconv.Atoi(string(command.Get(2)))

	success, err := redis.keys.SetExpire(key, seconds*1000)
	if success {
		return writer.WriteInt(1)
	} else {
		if err != nil {
			log.Println("ERROR: ", err.Error())
		}
		return writer.WriteInt(0)
	}
}

type ttlCommand struct{}

func (cmd *ttlCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	success, resp, err := redis.strings.Get(key)
	if success && resp.WillExpire() {
		return writer.WriteInt(resp.TTLInSeconds())
	} else if success {
		return writer.WriteInt(-1) // the key exists, but it won't expire
	} else if !success && err == nil {
		return writer.WriteInt(-2) // the key didn't exist
	} else {
		panic(err) // TODO ergh
	}
}
