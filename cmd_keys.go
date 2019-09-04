package pgredis

import (
	"log"
	"strconv"

	"github.com/secmask/go-redisproto"
)

type delCommand struct{}

func (cmd *delCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	result := int64(0)
	for i := 1; i < command.ArgCount(); i++ {
		// TODO calling Delete in a loop like this returns the correct result, but is super
		//      inefficient. It'd be better to delete them in a single SQL call
		success, err := redis.keys.Delete(command.Get(i))
		if err != nil {
			log.Println("ERROR: ", err.Error())
		}
		if success {
			result += 1
		}
	}
	return writer.WriteInt(result)
}

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
	// this should probably use KeyRepository and not be string specific
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
