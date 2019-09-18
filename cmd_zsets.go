package pgredis

import (
	"database/sql"
	"log"
	"strconv"

	"github.com/secmask/go-redisproto"
)

type zaddCommand struct{}

func (cmd *zaddCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
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
		return newPgRedisError("XX arg provided, but not yet supported")
	}
	if nxArgProvided {
		return newPgRedisError("NX arg provided, but not yet supported")
	}
	if incrArgProvided {
		return newPgRedisError("INCR arg provided, but not yet supported")
	}

	updated, err := redis.sortedsets.Add(tx, key, values, chArgProvided)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	} else {
		return newPgRedisInt(updated)
	}
}

type zcardCommand struct{}

func (cmd *zcardCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)

	count, err := redis.sortedsets.Cardinality(tx, key)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	} else {
		return newPgRedisInt(count)
	}
}

type zrangeCommand struct{}

func (cmd *zrangeCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	start, _ := strconv.Atoi(string(command.Get(2)))
	end, _ := strconv.Atoi(string(command.Get(3)))
	includeScores := string(command.Get(4)) == "WITHSCORES"

	items, err := redis.sortedsets.Range(tx, key, start, end, includeScores)
	if err == nil {
		return newPgRedisArrayOfStrings(items)
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	}
}

type zremCommand struct{}

func (cmd *zremCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	values := make([][]byte, 0)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}

	updated, err := redis.sortedsets.Remove(tx, key, values)

	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisError(err.Error())
	} else {
		return newPgRedisInt(updated)
	}
}
