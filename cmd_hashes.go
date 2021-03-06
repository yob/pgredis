package pgredis

import (
	"database/sql"
	"log"
)

type hgetCommand struct{}

func (cmd *hgetCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	field := command.Get(2)
	success, value, err := redis.hashes.Get(tx, key, field)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return nil, err
	}
	if success {
		return newPgRedisString(string(value)), nil
	} else {
		return newPgRedisNil(), nil
	}
}

func (cmd *hgetCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type hmgetCommand struct{}

func (cmd *hmgetCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
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

func (cmd *hmgetCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type hgetallCommand struct{}

func (cmd *hgetallCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	fields_and_values, err := redis.hashes.GetAll(tx, key)
	if err != nil {
		return nil, err
	}
	return newPgRedisArrayOfStrings(fields_and_values), nil
}

func (cmd *hgetallCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type hmsetCommand struct{}

func (cmd *hmsetCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := string(command.Get(1))
	items := make(map[string]string)

	for i := 2; i < command.ArgCount(); i += 2 {
		items[string(command.Get(i))] = string(command.Get(i + 1))
	}
	err := redis.hashes.SetMultiple(tx, key, items)
	if err != nil {
		return nil, err
	}
	return newPgRedisString("OK"), nil
}

func (cmd *hmsetCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type hsetCommand struct{}

func (cmd *hsetCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	field := command.Get(2)
	value := command.Get(3)
	inserted, err := redis.hashes.Set(tx, key, field, value)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(inserted), nil
}

func (cmd *hsetCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}
