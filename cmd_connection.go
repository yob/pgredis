package pgredis

import (
	"database/sql"
)

type echoCommand struct{}

func (cmd *echoCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	arg := command.Get(1)
	return newPgRedisString(string(arg)), nil
}

func (cmd *echoCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type pingCommand struct{}

func (cmd *pingCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	arg := command.Get(1)
	if len(arg) == 0 {
		return newPgRedisString("PONG"), nil
	} else {
		return newPgRedisString(string(arg)), nil
	}
}

func (cmd *pingCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type quitCommand struct{}

func (cmd *quitCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	return newPgRedisString("OK"), nil
}

func (cmd *quitCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type selectCommand struct{}

func (cmd *selectCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	return newPgRedisString("OK"), nil
}

func (cmd *selectCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}
