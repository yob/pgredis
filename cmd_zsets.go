package pgredis

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
)

type zaddCommand struct{}

func (cmd *zaddCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	xxArgProvided := false
	nxArgProvided := false
	chArgProvided := false
	incrArgProvided := false
	lastArg := ""
	values := make(map[string]float64)
	key := command.Get(1)

	for i := 2; i < command.ArgCount(); i++ {
		if lastArg != "" { // the previous arg was a score, so this must be a member
			score, _ := strconv.ParseFloat(lastArg, 64)
			values[string(command.Get(i))] = score
			lastArg = ""
		} else if string(command.Get(i)) == "XX" {
			xxArgProvided = true
		} else if string(command.Get(i)) == "NX" {
			nxArgProvided = true
		} else if string(command.Get(i)) == "CH" {
			chArgProvided = true
		} else if string(command.Get(i)) == "INCR" {
			incrArgProvided = true
		} else { // must be a score
			lastArg = string(command.Get(i))
		}
	}

	if xxArgProvided {
		return nil, errors.New("XX arg provided, but not yet supported")
	}
	if nxArgProvided {
		return nil, errors.New("NX arg provided, but not yet supported")
	}
	if incrArgProvided {
		return nil, errors.New("INCR arg provided, but not yet supported")
	}

	updated, err := redis.sortedsets.Add(tx, key, values, chArgProvided)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(updated), nil
}

type zcardCommand struct{}

func (cmd *zcardCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)

	count, err := redis.sortedsets.Cardinality(tx, key)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(count), nil
}

type zrangeCommand struct{}

func (cmd *zrangeCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	start, _ := strconv.Atoi(string(command.Get(2)))
	end, _ := strconv.Atoi(string(command.Get(3)))
	includeScores := string(command.Get(4)) == "WITHSCORES"

	items, err := redis.sortedsets.Range(tx, key, start, end, "asc", includeScores)
	if err != nil {
		return nil, err
	}
	return newPgRedisArrayOfStrings(items), nil
}

type zrangebyscoreCommand struct{}

func (cmd *zrangebyscoreCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	var min float64
	var max float64
	var minExclusive bool
	var maxExclusive bool
	var offset, count int

	key := command.Get(1)
	minString := string(command.Get(2))
	maxString := string(command.Get(3))
	includeScores := commandHasValue(command, "WITHSCORES")

	if strings.HasPrefix(minString, "(") {
		minExclusive = true
		min, _ = strconv.ParseFloat(minString[1:len(minString)], 64)
	} else {
		minExclusive = false
		min, _ = strconv.ParseFloat(minString, 64)
	}

	if strings.HasPrefix(maxString, "(") {
		maxExclusive = true
		max, _ = strconv.ParseFloat(maxString[1:len(maxString)], 64)
	} else {
		maxExclusive = false
		max, _ = strconv.ParseFloat(maxString, 64)
	}

	if commandHasValue(command, "LIMIT") {
		offset, count = commandLimitOffsetAndCount(command)
	}
	items, err := redis.sortedsets.RangeByScore(tx, key, min, minExclusive, max, maxExclusive, offset, count, includeScores)
	if err != nil {
		return nil, err
	}

	return newPgRedisArrayOfStrings(items), nil
}

type zremCommand struct{}

func (cmd *zremCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	values := make([][]byte, 0)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}

	updated, err := redis.sortedsets.Remove(tx, key, values)

	if err != nil {
		return nil, err
	}
	return newPgRedisInt(updated), nil
}

type zremrangebyrankCommand struct{}

func (cmd *zremrangebyrankCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	start, _ := strconv.Atoi(string(command.Get(2)))
	end, _ := strconv.Atoi(string(command.Get(3)))

	removed, err := redis.sortedsets.RemoveRangeByRank(tx, key, start, end)
	if err != nil {
		return nil, err
	}

	return newPgRedisInt(removed), nil
}

type zremrangebyscoreCommand struct{}

func (cmd *zremrangebyscoreCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	var min float64
	var max float64
	var minExclusive bool
	var maxExclusive bool

	key := command.Get(1)
	minString := string(command.Get(2))
	maxString := string(command.Get(3))

	if strings.HasPrefix(minString, "(") {
		minExclusive = true
		min, _ = strconv.ParseFloat(minString[1:len(minString)], 64)
	} else {
		minExclusive = false
		min, _ = strconv.ParseFloat(minString, 64)
	}

	if strings.HasPrefix(maxString, "(") {
		maxExclusive = true
		max, _ = strconv.ParseFloat(maxString[1:len(maxString)], 64)
	} else {
		maxExclusive = false
		max, _ = strconv.ParseFloat(maxString, 64)
	}

	removed, err := redis.sortedsets.RemoveRangeByScore(tx, key, min, minExclusive, max, maxExclusive)
	if err != nil {
		return nil, err
	}

	return newPgRedisInt(removed), nil
}

type zrevrangeCommand struct{}

func (cmd *zrevrangeCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	start, _ := strconv.Atoi(string(command.Get(2)))
	end, _ := strconv.Atoi(string(command.Get(3)))
	includeScores := string(command.Get(4)) == "WITHSCORES"

	items, err := redis.sortedsets.Range(tx, key, start, end, "desc", includeScores)
	if err != nil {
		return nil, err
	}
	return newPgRedisArrayOfStrings(items), nil
}

func commandLimitOffsetAndCount(command *redisRequest) (int, int) {
	indexOfLimit := indexOfValue(command, "LIMIT")
	if indexOfLimit == 0 {
		return 0, 0
	} else {
		offset, offsetErr := strconv.Atoi(string(command.Get(indexOfLimit + 1)))
		count, countErr := strconv.Atoi(string(command.Get(indexOfLimit + 2)))
		if offsetErr != nil || countErr != nil {
			return 0, 0
		}
		return offset, count
	}
}
