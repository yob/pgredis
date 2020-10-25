package pgredis

import (
	"database/sql"
)

type saddCommand struct{}

func (cmd *saddCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	values := make([][]byte, 0)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}

	updated, err := redis.sets.Add(tx, key, values)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(updated), nil
}

type scardCommand struct{}

func (cmd *scardCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)

	count, err := redis.sets.Cardinality(tx, key)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(count), nil
}

type sremCommand struct{}

func (cmd *sremCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	values := make([][]byte, 0)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}

	updated, err := redis.sets.Remove(tx, key, values)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(updated), nil
}

type smembersCommand struct{}

func (cmd *smembersCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)

	values, err := redis.sets.Members(tx, key)
	if err != nil {
		return nil, err
	}
	return newPgRedisArrayOfStrings(values), nil
}

type sscanCommand struct{}

func (cmd *sscanCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)

	values, err := redis.sets.Members(tx, key)
	if err != nil {
		return nil, err
	}
	response := make([]pgRedisValue, 0)
	response = append(response, newPgRedisString("0"))
	response = append(response, newPgRedisArrayOfStrings(values))
	return newPgRedisArray(response), nil
}
