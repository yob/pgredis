package pgredis

import (
	"database/sql"
	"fmt"
	"github.com/yob/go-redisproto"
	"strconv"
	"time"
)

type brpopCommand struct{}

// TODO this sleeping approach might work, but it's lame. It'd be neat to use psql NOTIFY
// to be informed when a list is ready to rpop
func (cmd *brpopCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	listCount := command.ArgCount() - 2
	listKeys := []string{}
	for i := 1; i <= listCount; i++ {
		listKeys = append(listKeys, string(command.Get(i)))
	}
	fmt.Printf("%d %v\n", listCount, listKeys)
	startTime := time.Now()
	timeout := fmt.Sprintf("%ss", string(command.Get(2)))
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

type llenCommand struct{}

func (cmd *llenCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	length, err := redis.lists.Length(tx, key)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(int64(length)), nil
}

type lpopCommand struct{}

func (cmd *lpopCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
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

type lpushCommand struct{}

func (cmd *lpushCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
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

type lrangeCommand struct{}

func (cmd *lrangeCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
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

type lremCommand struct{}

func (cmd *lremCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	count, _ := strconv.Atoi(string(command.Get(2)))
	value := command.Get(3)
	removed_count, err := redis.lists.LeftRemove(tx, key, count, value)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(removed_count), nil
}

type rpopCommand struct{}

func (cmd *rpopCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
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

type rpushCommand struct{}

func (cmd *rpushCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
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
