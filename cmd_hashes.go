package pgredis

import (
	"log"

	"github.com/secmask/go-redisproto"
)

type hgetCommand struct{}

func (cmd *hgetCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	field := command.Get(2)
	success, value, err := redis.hashes.Get(key, field)
	if success {
		return writer.WriteBulkString(string(value))
	} else if !success && err == nil {
		return writer.WriteBulk(nil)
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

type hmgetCommand struct{}

func (cmd *hmgetCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	values := make([]interface{}, command.ArgCount()-2)
	for i := 2; i < command.ArgCount(); i++ {
		// TODO calling Get in a loop like this returns the correct result, but is super inefficient
		success, value, _ := redis.hashes.Get(key, command.Get(i))
		if success {
			values[i-2] = string(value)
		}
	}
	return writer.WriteObjectsSlice(values)
}

type hgetallCommand struct{}

func (cmd *hgetallCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	fields_and_values, err := redis.hashes.GetAll(key)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return writer.WriteError(err.Error())
	} else {
		return writer.WriteBulkStrings(fields_and_values)
	}
}

type hmsetCommand struct{}

func (cmd *hmsetCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := string(command.Get(1))
	items := make(map[string]string)

	for i := 2; i < command.ArgCount(); i += 2 {
		items[string(command.Get(i))] = string(command.Get(i + 1))
	}
	log.Printf("items: %v\n", items)
	err := redis.hashes.SetMultiple(key, items)
	if err == nil {
		return writer.WriteBulkString("OK")
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

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
