package pgredis

import (
	"database/sql"
	"log"
	"strconv"

	"github.com/secmask/go-redisproto"
)

type llenCommand struct{}

func (cmd *llenCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	length, err := redis.lists.Length(tx, key)
	if err != nil {
		return newPgRedisError(err.Error())
	}
	return newPgRedisInt(int64(length))
}

type lpushCommand struct{}

func (cmd *lpushCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	values := make([][]byte, 0)
	key := command.Get(1)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}
	newLength, err := redis.lists.LeftPush(tx, key, values)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	}
	return newPgRedisInt(int64(newLength))
}

type lrangeCommand struct{}

func (cmd *lrangeCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	start, _ := strconv.Atoi(string(command.Get(2)))
	end, _ := strconv.Atoi(string(command.Get(3)))
	items, err := redis.lists.Lrange(tx, key, start, end)
	if err == nil {
		return newPgRedisArrayOfStrings(items)
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	}
}

type lremCommand struct{}

func (cmd *lremCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	count, _ := strconv.Atoi(string(command.Get(2)))
	value := command.Get(3)
	removed_count, err := redis.lists.LeftRemove(tx, key, count, value)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	}
	return newPgRedisInt(removed_count)
}

type rpopCommand struct{}

func (cmd *rpopCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	success, value, err := redis.lists.RightPop(tx, key)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	}
	if success {
		return newPgRedisString(string(value))
	} else {
		return newPgRedisNil()
	}
}

type rpushCommand struct{}

func (cmd *rpushCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	values := make([][]byte, 0)
	key := command.Get(1)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}
	newLength, err := redis.lists.RightPush(tx, key, values)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	}
	return newPgRedisInt(int64(newLength))
}
