package pgredis

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type flushallCommand struct{}

func (cmd *flushallCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	err := redis.keys.FlushAll(tx)
	if err != nil {
		return nil, err
	}
	return newPgRedisString("OK"), nil
}

func (cmd *flushallCommand) keysToLock(command *redisRequest) []string {
	// TODO do we need to lock on all keys/tables?
	return []string{}
}

type clientCommand struct{}

func (cmd *clientCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	subcommand := strings.ToUpper(string(command.Get(1)))
	if subcommand == "SETNAME" {
		return newPgRedisString("OK"), nil
	} else {
		return nil, errors.New(fmt.Sprintf("Unrecognised client subcommand: %s", subcommand))
	}
}

func (cmd *clientCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type dbsizeCommand struct{}

func (cmd *dbsizeCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	count, err := redis.keys.Count(tx)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(count), nil
}

func (cmd *dbsizeCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type infoCommand struct{}

func (cmd *infoCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	result := make([]string, 0)
	result = append(result, "# Server")
	result = append(result, "redis_version:5.0.5")
	result = append(result, "uptime_in_days:0")
	result = append(result, "# Clients")
	result = append(result, "connected_clients:1")
	result = append(result, "# Memory")
	result = append(result, "used_memory_human:834.12K")
	result = append(result, "used_memory_peak_human:834.12K")
	result = append(result, "# Persistence")
	result = append(result, "# Stats")
	result = append(result, fmt.Sprintf("total_connections_received:%d", redis.connCount))
	result = append(result, "# Replication")
	result = append(result, "# CPU")
	result = append(result, "# Cluster")
	result = append(result, "cluster_enabled:0")
	result = append(result, "# Keyspace")
	return newPgRedisString(strings.Join(result, "\r\n")), nil
}

func (cmd *infoCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}
