package pgredis

import (
	"database/sql"
	"log"

	"github.com/secmask/go-redisproto"
)

type saddCommand struct{}

func (cmd *saddCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	values := make([][]byte, 0)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}

	updated, err := redis.sets.Add(tx, key, values)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	} else {
		return newPgRedisInt(updated)
	}
}

type scardCommand struct{}

func (cmd *scardCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)

	count, err := redis.sets.Cardinality(tx, key)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	} else {
		return newPgRedisInt(count)
	}
}

type sremCommand struct{}

func (cmd *sremCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	values := make([][]byte, 0)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}

	updated, err := redis.sets.Remove(tx, key, values)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	} else {
		return newPgRedisInt(updated)
	}
}

type smembersCommand struct{}

func (cmd *smembersCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)

	values, err := redis.sets.Members(tx, key)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	} else {
		return newPgRedisArrayOfStrings(values)
	}
}
