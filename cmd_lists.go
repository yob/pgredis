package pgredis

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

type brpopCommand struct{}

// TODO this sleeping approach might work, but it's lame. It'd be neat to use psql NOTIFY
// to be informed when a list is ready to rpop
func (cmd *brpopCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	listCount := command.ArgCount() - 2
	listKeys := []string{}
	for i := 1; i <= listCount; i++ {
		listKeys = append(listKeys, string(command.Get(i)))
	}
	startTime := time.Now()
	timeout := fmt.Sprintf("%ss", string(command.Get(command.ArgCount()-1)))
	maxDuration, _ := time.ParseDuration(timeout)
	for {
		for _, key := range listKeys {
			success, value, err := redis.lists.RightPop(tx, []byte(key))
			if err != nil {
				return nil, err
			}
			if success {
				items := []string{string(key), string(value)}
				return newPgRedisArrayOfStrings(items), nil
			}
			if time.Since(startTime) > maxDuration {
				return newPgRedisNilArray(), nil
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (cmd *brpopCommand) keysToLock(command *redisRequest) []string {
	// TODO we don't claim any locks here, because we don't want to block other connections
	//      while we poll. This makes this command open to deadlocks.
	return []string{}
}

type llenCommand struct{}

func (cmd *llenCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	length, err := redis.lists.Length(tx, key)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(int64(length)), nil
}

func (cmd *llenCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type lpopCommand struct{}

func (cmd *lpopCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	success, value, err := redis.lists.LeftPop(tx, key)
	if err != nil {
		return nil, err
	}
	if success {
		return newPgRedisString(string(value)), nil
	} else {
		return newPgRedisNil(), nil
	}
}

func (cmd *lpopCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type lpushCommand struct{}

func (cmd *lpushCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	values := make([][]byte, 0)
	key := command.Get(1)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}
	newLength, err := redis.lists.LeftPush(tx, key, values)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(int64(newLength)), nil
}

func (cmd *lpushCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type lrangeCommand struct{}

func (cmd *lrangeCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	start, _ := strconv.Atoi(string(command.Get(2)))
	end, _ := strconv.Atoi(string(command.Get(3)))
	items, err := redis.lists.Lrange(tx, key, start, end)
	if err == nil {
		return newPgRedisArrayOfStrings(items), nil
	} else {
		return nil, err
	}
}

func (cmd *lrangeCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type lremCommand struct{}

func (cmd *lremCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	count, _ := strconv.Atoi(string(command.Get(2)))
	value := command.Get(3)
	removed_count, err := redis.lists.LeftRemove(tx, key, count, value)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(removed_count), nil
}

func (cmd *lremCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type rpopCommand struct{}

func (cmd *rpopCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	success, value, err := redis.lists.RightPop(tx, key)
	if err != nil {
		return nil, err
	}
	if success {
		return newPgRedisString(string(value)), nil
	} else {
		return newPgRedisNil(), nil
	}
}

func (cmd *rpopCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type rpushCommand struct{}

func (cmd *rpushCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	values := make([][]byte, 0)
	key := command.Get(1)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}
	newLength, err := redis.lists.RightPush(tx, key, values)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(int64(newLength)), nil
}

func (cmd *rpushCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}
