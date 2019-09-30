package pgredis

import (
	"database/sql"
	"log"
	"strconv"

	"github.com/secmask/go-redisproto"
)

type delCommand struct{}

func (cmd *delCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	result := int64(0)
	for i := 1; i < command.ArgCount(); i++ {
		// TODO calling Delete in a loop like this returns the correct result, but is super
		//      inefficient. It'd be better to delete them in a single SQL call
		success, err := redis.keys.Delete(tx, command.Get(i))
		if err != nil {
			log.Println("ERROR: ", err.Error())
		}
		if success {
			result += 1
		}
	}
	return newPgRedisInt(result), nil
}

type existsCommand struct{}

func (cmd *existsCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	result := int64(0)
	for i := 1; i < command.ArgCount(); i++ {
		success, err := redis.keys.Exist(tx, command.Get(i))
		if err != nil {
			log.Println("ERROR: ", err.Error())
		}
		if success {
			result += 1
		}
	}
	return newPgRedisInt(result), nil
}

type expireCommand struct{}

func (cmd *expireCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	seconds, _ := strconv.Atoi(string(command.Get(2)))

	success, err := redis.keys.SetExpire(tx, key, seconds)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisInt(0), nil
	}
	if success {
		return newPgRedisInt(1), nil
	} else {
		return newPgRedisInt(0), nil
	}
}

type pttlCommand struct{}

func (cmd *pttlCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	// this should probably use KeyRepository and not be string specific
	keyExists, millis, err := redis.keys.TTLInMillis(tx, key)
	if err != nil {
		return nil, err
	}
	if keyExists && millis > 0 {
		return newPgRedisInt(millis), nil
	} else if keyExists {
		return newPgRedisInt(-1), nil // the key exists, but it won't expire
	} else {
		return newPgRedisInt(-2), nil // the key didn't exist
	}
}

type ttlCommand struct{}

func (cmd *ttlCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	// this should probably use KeyRepository and not be string specific
	success, resp, err := redis.strings.Get(tx, key)
	if err != nil {
		return nil, err
	}
	if success && resp.WillExpire() {
		return newPgRedisInt(resp.TTLInSeconds()), nil
	} else if success {
		return newPgRedisInt(-1), nil // the key exists, but it won't expire
	} else {
		return newPgRedisInt(-2), nil // the key didn't exist
	}
}

type typeCommand struct{}

func (cmd *typeCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	result, err := redis.keys.Type(tx, key)
	if err != nil {
		return nil, err
	}
	if result != "" {
		return newPgRedisString(result), nil
	} else {
		return newPgRedisString("none"), nil
	}
}
