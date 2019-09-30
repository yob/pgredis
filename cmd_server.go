package pgredis

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/secmask/go-redisproto"
	"strings"
)

type flushallCommand struct{}

func (cmd *flushallCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	err := redis.keys.FlushAll(tx)
	if err != nil {
		return nil, err
	}
	return newPgRedisString("OK"), nil
}

type clientCommand struct{}

func (cmd *clientCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	subcommand := strings.ToUpper(string(command.Get(1)))
	if subcommand == "SETNAME" {
		return newPgRedisString("OK"), nil
	} else {
		return nil, errors.New(fmt.Sprintf("Unrecognised client subcommand: %s", subcommand))
	}
}

type dbsizeCommand struct{}

func (cmd *dbsizeCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	count, err := redis.keys.Count(tx)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(count), nil
}

type infoCommand struct{}

func (cmd *infoCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
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
