package pgredis

import (
	"database/sql"
	"log"
	"strconv"

	"github.com/secmask/go-redisproto"
)

type delCommand struct{}

func (cmd *delCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
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
	return newPgRedisInt(result)
}

type existsCommand struct{}

func (cmd *existsCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
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
	return newPgRedisInt(result)
}

type expireCommand struct{}

func (cmd *expireCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	seconds, _ := strconv.Atoi(string(command.Get(2)))

	success, err := redis.keys.SetExpire(tx, key, seconds*1000)
	if success {
		return newPgRedisInt(1)
	} else {
		if err != nil {
			log.Println("ERROR: ", err.Error())
		}
		return newPgRedisInt(0)
	}
}

type ttlCommand struct{}

func (cmd *ttlCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	// this should probably use KeyRepository and not be string specific
	success, resp, err := redis.strings.Get(tx, key)
	if success && resp.WillExpire() {
		return newPgRedisInt(resp.TTLInSeconds())
	} else if success {
		return newPgRedisInt(-1) // the key exists, but it won't expire
	} else if !success && err == nil {
		return newPgRedisInt(-2) // the key didn't exist
	} else {
		return newPgRedisError(err.Error())
	}
}

type typeCommand struct{}

func (cmd *typeCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	result, err := redis.keys.Type(tx, key)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	}
	if result != "" {
		return newPgRedisString(result)
	} else {
		return newPgRedisString("none")
	}
}
