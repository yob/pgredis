package pgredis

import (
	"bytes"
	"database/sql"
	"log"
	"strconv"

	"github.com/32bitkid/bitreader"
)

type appendCommand struct{}

func (cmd *appendCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	value := command.Get(2)
	newValue, err := redis.strings.InsertOrAppend(tx, key, value)
	if err != nil {
		return nil, err
	}
	return newPgRedisInt(int64(len(newValue))), nil
}

func (cmd *appendCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type bitcountCommand struct{}

func intOrZero(value string) int {
	if value == "" {
		return 0
	} else {
		result, err := strconv.Atoi(value)
		if err != nil {
			return 0
		}
		return result
	}
}

func (cmd *bitcountCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	success, result, err := redis.strings.Get(tx, key)

	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisInt(0), nil // TODO probbly not right
	}
	if success {
		start, _ := strconv.Atoi(string(command.Get(2)))
		end, _ := strconv.Atoi(string(command.Get(3)))

		if start < 0 {
			start = len(result.Value) + start
		}

		if end < 0 {
			end = len(result.Value) + end
		}

		end += 1

		if end < start {
			end = start
		}

		if start > len(result.Value) {
			start = len(result.Value)
		}

		if end > len(result.Value) {
			end = len(result.Value)
		}
		bitsToRead := (end - start) * 8

		byteReader := bytes.NewReader(result.Value)
		bitReader := bitreader.NewReader(byteReader)
		bitReader.Skip(uint(start))
		setCount := int64(0)

		for i := 0; i < bitsToRead; i++ {
			bitSet, bitErr := bitReader.Read1()
			if bitErr != nil {
				break
			}
			if bitSet {
				setCount += 1
			}
		}
		return newPgRedisInt(setCount), nil
	} else {
		return newPgRedisInt(0), nil // assumed to be an empty string, with all 0 bits
	}
}

func (cmd *bitcountCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type decrCommand struct{}

func (cmd *decrCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	newValue, err := redis.strings.Incr(tx, key, -1)
	if err != nil {
		return nil, err
	}
	intValue, _ := strconv.Atoi(string(newValue))
	return newPgRedisInt(int64(intValue)), nil
}

func (cmd *decrCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type decrbyCommand struct{}

func (cmd *decrbyCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	by, _ := strconv.Atoi(string(command.Get(2)))
	newValue, err := redis.strings.Incr(tx, key, by*-1)
	if err != nil {
		return nil, err
	}
	intValue, _ := strconv.Atoi(string(newValue))
	return newPgRedisInt(int64(intValue)), nil
}

func (cmd *decrbyCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type getCommand struct{}

func (cmd *getCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	success, resp, err := redis.strings.Get(tx, command.Get(1))
	if err != nil {
		return nil, err
	}
	if success {
		return newPgRedisString(string(resp.Value)), nil
	} else {
		return newPgRedisNil(), nil
	}
}

func (cmd *getCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type getbitCommand struct{}

func (cmd *getbitCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	success, resp, err := redis.strings.Get(tx, command.Get(1))
	bitPosition, _ := strconv.Atoi(string(command.Get(2)))

	if err != nil {
		log.Println("ERROR: ", err.Error())
		return newPgRedisInt(0), nil // TODO probbly not right
	}
	if success {
		byteReader := bytes.NewReader(resp.Value)
		bitReader := bitreader.NewReader(byteReader)
		bitReader.Skip(uint(bitPosition))
		bitSet, err := bitReader.Read1()
		if err != nil {
			log.Println("ERROR: ", err.Error())
			return newPgRedisInt(0), nil // assumed to be an empty string, with all 0 bits
		}
		if bitSet {
			return newPgRedisInt(1), nil
		} else {
			return newPgRedisInt(0), nil
		}
	} else {
		return newPgRedisInt(0), nil // assumed to be an empty string, with all 0 bits
	}
}

func (cmd *getbitCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type getsetCommand struct{}

func (cmd *getsetCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	expiry_millis := 0
	getSuccess, resp, err := redis.strings.Get(tx, command.Get(1))

	if err != nil {
		return nil, err
	}

	insertErr := redis.strings.InsertOrUpdate(tx, command.Get(1), command.Get(2), expiry_millis)
	if insertErr != nil {
		return nil, err
	}
	if getSuccess {
		return newPgRedisString(string(resp.Value)), nil
	} else {
		return newPgRedisNil(), nil
	}
}

func (cmd *getsetCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type getrangeCommand struct{}

func (cmd *getrangeCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	success, result, err := redis.strings.Get(tx, command.Get(1))
	if err != nil {
		return nil, err
	}
	if success {
		start, _ := strconv.Atoi(string(command.Get(2)))
		end, _ := strconv.Atoi(string(command.Get(3)))

		if start < 0 {
			start = len(result.Value) + start
		}

		if end < 0 {
			end = len(result.Value) + end
		}

		end += 1

		if end < start {
			end = start
		}

		if start > len(result.Value) {
			start = len(result.Value)
		}

		if end > len(result.Value) {
			end = len(result.Value)
		}

		return newPgRedisString(string(result.Value[start:end])), nil
	} else {
		return newPgRedisNil(), nil
	}
}

func (cmd *getrangeCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type incrCommand struct{}

func (cmd *incrCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	newValue, err := redis.strings.Incr(tx, key, 1)
	if err != nil {
		return nil, err
	}
	intValue, _ := strconv.Atoi(string(newValue))
	return newPgRedisInt(int64(intValue)), nil
}

func (cmd *incrCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type incrbyCommand struct{}

func (cmd *incrbyCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	by, _ := strconv.Atoi(string(command.Get(2)))
	newValue, err := redis.strings.Incr(tx, key, by)
	if err != nil {
		return nil, err
	}
	intValue, _ := strconv.Atoi(string(newValue))
	return newPgRedisInt(int64(intValue)), nil
}

func (cmd *incrbyCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type incrbyfloatCommand struct{}

func (cmd *incrbyfloatCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	by, _ := strconv.ParseFloat(string(command.Get(2)), 64)
	newValue, err := redis.strings.IncrDecimal(tx, key, by)
	if err != nil {
		return nil, err
	}
	return newPgRedisString(string(newValue)), nil
}

func (cmd *incrbyfloatCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type mgetCommand struct{}

func (cmd *mgetCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	result := make([]pgRedisValue, command.ArgCount()-1)
	for i := 1; i < command.ArgCount(); i++ {
		// TODO calling getStrings in a loop like this returns the correct result, but is super
		//      inefficient
		success, resp, _ := redis.strings.Get(tx, command.Get(i))
		if success {
			result[i-1] = newPgRedisString(string(resp.Value))
		} else {
			result[i-1] = newPgRedisNil()
		}
	}
	return newPgRedisArray(result), nil
}

func (cmd *mgetCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

type msetCommand struct{}

func (cmd *msetCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	// TODO Using string because I can't use byte slices as a map key, but this probably breaks
	// some compatibility with redis
	items := make(map[string]string)
	for i := 1; i < command.ArgCount(); i += 2 {
		items[string(command.Get(i))] = string(command.Get(i + 1))
	}
	err := redis.strings.InsertOrUpdateMultiple(tx, items)
	if err != nil {
		return nil, err
	}
	return newPgRedisString("OK"), nil
}

func (cmd *msetCommand) keysToLock(command *redisRequest) []string {
	// MSET foo 1 bar 2 => {foo, bar}
	result := []string{}
	for i, arg := range command.Args()[1:] {
		if i%2 == 0 {
			result = append(result, arg)
		}
	}
	return result
}

type setCommand struct{}

func (cmd *setCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	expiry_millis := 0
	exValue := commandExValueInMillis(command)
	if exValue > 0 {
		expiry_millis = exValue
	}
	pxValue := commandPxValueInMillis(command)
	if pxValue > 0 {
		expiry_millis = pxValue
	}

	xxArgProvided := commandHasValue(command, "XX")
	nxArgProvided := commandHasValue(command, "NX")
	if xxArgProvided { // only set the key if it already exists
		updated, err := redis.strings.UpdateOrSkip(tx, command.Get(1), command.Get(2), expiry_millis)
		if err != nil {
			return nil, err
		}
		if updated {
			return newPgRedisString("OK"), nil
		} else {
			return newPgRedisNil(), nil
		}
	} else if nxArgProvided { // only set the key if it doesn't already exists
		updated, err := redis.strings.InsertOrSkip(tx, command.Get(1), command.Get(2), expiry_millis)
		if err != nil {
			return nil, err
		}
		if updated {
			return newPgRedisString("OK"), nil
		} else {
			return newPgRedisNil(), nil
		}
	} else {
		err := redis.strings.InsertOrUpdate(tx, command.Get(1), command.Get(2), expiry_millis)
		if err != nil {
			return nil, err
		}
		return newPgRedisString("OK"), nil
	}
}

func (cmd *setCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type setexCommand struct{}

func (cmd *setexCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	expiry_secs, _ := strconv.Atoi(string(command.Get(2)))
	value := command.Get(3)
	expiry_millis := expiry_secs * 1000

	err := redis.strings.InsertOrUpdate(tx, key, value, expiry_millis)
	if err != nil {
		return nil, err
	}
	return newPgRedisString("OK"), nil
}

func (cmd *setexCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type psetexCommand struct{}

func (cmd *psetexCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	expiry_millis, _ := strconv.Atoi(string(command.Get(2)))
	value := command.Get(3)
	err := redis.strings.InsertOrUpdate(tx, key, value, expiry_millis)
	if err != nil {
		return nil, err
	}
	return newPgRedisString("OK"), nil
}

func (cmd *psetexCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type setnxCommand struct{}

func (cmd *setnxCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	key := command.Get(1)
	value := command.Get(2)
	expiry_millis := 0

	updated, err := redis.strings.InsertOrSkip(tx, key, value, expiry_millis)
	if err != nil {
		return nil, err
	}
	if updated {
		return newPgRedisString("OK"), nil
	} else {
		return newPgRedisNil(), nil
	}
}

func (cmd *setnxCommand) keysToLock(command *redisRequest) []string {
	return command.Args()[1:2]
}

type strlenCommand struct{}

func (cmd *strlenCommand) Execute(command *redisRequest, redis *PgRedis, tx *sql.Tx) (pgRedisValue, error) {
	success, resp, err := redis.strings.Get(tx, command.Get(1))
	if err != nil {
		return nil, err
	}
	if success {
		return newPgRedisInt(int64(len(resp.Value))), nil
	} else {
		return newPgRedisInt(0), nil
	}
}

func (cmd *strlenCommand) keysToLock(command *redisRequest) []string {
	return []string{}
}

func commandExValueInMillis(command *redisRequest) int {
	indexOfEx := indexOfValue(command, "EX")
	if indexOfEx == 0 {
		return 0
	} else {
		seconds, err := strconv.Atoi(string(command.Get(indexOfEx + 1)))
		if err == nil {
			return seconds * 1000
		} else {
			return 0
		}
	}
}

func commandPxValueInMillis(command *redisRequest) int {
	indexOfPx := indexOfValue(command, "PX")
	if indexOfPx == 0 {
		return 0
	} else {
		millis, err := strconv.Atoi(string(command.Get(indexOfPx + 1)))
		if err == nil {
			return millis
		} else {
			return 0
		}
	}
}

func indexOfValue(command *redisRequest, value string) int {
	for i := 1; i < command.ArgCount(); i++ {
		if string(command.Get(i)) == value {
			return i
		}
	}
	return 0
}

func commandHasValue(command *redisRequest, value string) bool {
	for i := 1; i < command.ArgCount(); i++ {
		if string(command.Get(i)) == value {
			return true
		}
	}
	return false
}
