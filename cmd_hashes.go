package pgredis

import (
	"database/sql"
	"github.com/yob/go-redisproto"
	"log"
)

type hgetCommand struct{}

func (cmd *hgetCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	field := command.Get(2)
	success, value, err := redis.hashes.Get(tx, key, field)
	if success {
		return newPgRedisString(string(value)), nil
	} else if !success && err == nil {
		return newPgRedisNil(), nil
	} else {
		log.Println("ERROR: ", err.Error())
		return nil, err
	}
}

type hmgetCommand struct{}

func (cmd *hmgetCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	values := make([]pgRedisValue, command.ArgCount()-2)
	for i := 2; i < command.ArgCount(); i++ {
		// TODO calling Get in a loop like this returns the correct result, but is super inefficient
		success, value, _ := redis.hashes.Get(tx, key, command.Get(i))
		if success {
			values[i-2] = newPgRedisString(string(value))
		} else {
			values[i-2] = newPgRedisNil()
		}
	}
	return newPgRedisArray(values), nil
}

type hgetallCommand struct{}

func (cmd *hgetallCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	fields_and_values, err := redis.hashes.GetAll(tx, key)
	if err != nil {
		return nil, err
	} else {
		return newPgRedisArrayOfStrings(fields_and_values), nil
	}
}

type hmsetCommand struct{}

func (cmd *hmsetCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := string(command.Get(1))
	items := make(map[string]string)

	for i := 2; i < command.ArgCount(); i += 2 {
		items[string(command.Get(i))] = string(command.Get(i + 1))
	}
	err := redis.hashes.SetMultiple(tx, key, items)
	if err == nil {
		return newPgRedisString("OK"), nil
	} else {
		return nil, err
	}
}

type hsetCommand struct{}

func (cmd *hsetCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	field := command.Get(2)
	value := command.Get(3)
	inserted, err := redis.hashes.Set(tx, key, field, value)
	if err != nil {
		return nil, err
	} else {
		return newPgRedisInt(inserted), nil
	}
}
