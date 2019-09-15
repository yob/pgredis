package pgredis

import (
	"bytes"
	"database/sql"
	"log"
	"strconv"

	"github.com/32bitkid/bitreader"
	"github.com/secmask/go-redisproto"
)

type appendCommand struct{}

func (cmd *appendCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	value := command.Get(2)
	newValue, err := redis.strings.InsertOrAppend(tx, key, value)
	if err == nil {
		return newPgRedisInt(int64(len(newValue)))
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisNil()
	}
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

func (cmd *bitcountCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	success, result, err := redis.strings.Get(tx, key)

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
		return newPgRedisInt(setCount)
	} else if !success && err == nil {
		return newPgRedisInt(0) // assumed to be an empty string, with all 0 bits
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisInt(0) // TODO probbly not right
	}
}

type decrCommand struct{}

func (cmd *decrCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	newValue, err := redis.strings.Incr(tx, key, -1)
	if err == nil {
		intValue, _ := strconv.Atoi(string(newValue))
		return newPgRedisInt(int64(intValue))
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisNil()
	}
}

type decrbyCommand struct{}

func (cmd *decrbyCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	by, _ := strconv.Atoi(string(command.Get(2)))
	newValue, err := redis.strings.Incr(tx, key, by*-1)
	if err == nil {
		intValue, _ := strconv.Atoi(string(newValue))
		return newPgRedisInt(int64(intValue))
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisNil()
	}
}

type getCommand struct{}

func (cmd *getCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	success, resp, err := redis.strings.Get(tx, command.Get(1))
	if success {
		return newPgRedisString(string(resp.Value))
	} else if !success && err == nil {
		return newPgRedisNil()
	} else {
		return newPgRedisError(err.Error())
	}
}

type getbitCommand struct{}

func (cmd *getbitCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	success, resp, err := redis.strings.Get(tx, command.Get(1))
	bitPosition, _ := strconv.Atoi(string(command.Get(2)))

	if success {
		byteReader := bytes.NewReader(resp.Value)
		bitReader := bitreader.NewReader(byteReader)
		bitReader.Skip(uint(bitPosition))
		bitSet, err := bitReader.Read1()
		if err != nil {
			log.Println("ERROR: ", err.Error())
			return newPgRedisInt(0) // assumed to be an empty string, with all 0 bits
		}
		if bitSet {
			return newPgRedisInt(1)
		} else {
			return newPgRedisInt(0)
		}
	} else if !success && err == nil {
		return newPgRedisInt(0) // assumed to be an empty string, with all 0 bits
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisInt(0) // TODO probbly not right
	}
}

type getsetCommand struct{}

func (cmd *getsetCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	expiry_millis := 0
	getSuccess, resp, err := redis.strings.Get(tx, command.Get(1))

	insertErr := redis.strings.InsertOrUpdate(tx, command.Get(1), command.Get(2), expiry_millis)
	if insertErr == nil {
		if getSuccess {
			return newPgRedisString(string(resp.Value))
		} else if !getSuccess && err == nil {
			return newPgRedisNil()
		} else {
			return newPgRedisError(err.Error())
		}
	} else {
		log.Println("DB ERROR: ", err.Error())
		return newPgRedisNil()
	}
}

type getrangeCommand struct{}

func (cmd *getrangeCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	success, result, err := redis.strings.Get(tx, command.Get(1))
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

		return newPgRedisString(string(result.Value[start:end]))
	} else if !success && err == nil {
		return newPgRedisNil()
	} else {
		return newPgRedisError(err.Error())
	}
}

type incrCommand struct{}

func (cmd *incrCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	newValue, err := redis.strings.Incr(tx, key, 1)
	if err == nil {
		intValue, _ := strconv.Atoi(string(newValue))
		return newPgRedisInt(int64(intValue))
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisNil()
	}
}

type incrbyCommand struct{}

func (cmd *incrbyCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	by, _ := strconv.Atoi(string(command.Get(2)))
	newValue, err := redis.strings.Incr(tx, key, by)
	if err == nil {
		intValue, _ := strconv.Atoi(string(newValue))
		return newPgRedisInt(int64(intValue))
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisNil()
	}
}

type incrbyfloatCommand struct{}

func (cmd *incrbyfloatCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	by, _ := strconv.ParseFloat(string(command.Get(2)), 64)
	newValue, err := redis.strings.IncrDecimal(tx, key, by)
	if err == nil {
		return newPgRedisString(string(newValue))
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisNil()
	}
}

type mgetCommand struct{}

func (cmd *mgetCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
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
	return newPgRedisArray(result)
}

type msetCommand struct{}

func (cmd *msetCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	// TODO Using string because I can't use byte slices as a map key, but this probably breaks
	// some compatibility with redis
	items := make(map[string]string)
	for i := 1; i < command.ArgCount(); i += 2 {
		items[string(command.Get(i))] = string(command.Get(i + 1))
	}
	log.Printf("items: %v\n", items)
	err := redis.strings.InsertOrUpdateMultiple(tx, items)
	if err == nil {
		return newPgRedisString("OK")
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisNil()
	}
}

type setCommand struct{}

func (cmd *setCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
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
		if err == nil {
			if updated {
				return newPgRedisString("OK")
			} else {
				return newPgRedisNil()
			}
		} else {
			log.Println("ERROR: ", err.Error())
			return newPgRedisNil()
		}
	} else if nxArgProvided { // only set the key if it doesn't already exists
		updated, err := redis.strings.InsertOrSkip(tx, command.Get(1), command.Get(2), expiry_millis)
		if err == nil {
			if updated {
				return newPgRedisString("OK")
			} else {
				return newPgRedisNil()
			}
		} else {
			log.Println("ERROR: ", err.Error())
			return newPgRedisNil()
		}
	} else {
		err := redis.strings.InsertOrUpdate(tx, command.Get(1), command.Get(2), expiry_millis)
		if err == nil {
			return newPgRedisString("OK")
		} else {
			log.Println("ERROR: ", err.Error())
			return newPgRedisNil()
		}
	}
}

type setexCommand struct{}

func (cmd *setexCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	expiry_secs, _ := strconv.Atoi(string(command.Get(2)))
	value := command.Get(3)
	expiry_millis := expiry_secs * 1000

	err := redis.strings.InsertOrUpdate(tx, key, value, expiry_millis)
	if err == nil {
		return newPgRedisString("OK")
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisNil()
	}
}

type psetexCommand struct{}

func (cmd *psetexCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	expiry_millis, _ := strconv.Atoi(string(command.Get(2)))
	value := command.Get(3)
	err := redis.strings.InsertOrUpdate(tx, key, value, expiry_millis)
	if err == nil {
		return newPgRedisString("OK")
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisNil()
	}
}

type setnxCommand struct{}

func (cmd *setnxCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	key := command.Get(1)
	value := command.Get(2)
	expiry_millis := 0

	updated, err := redis.strings.InsertOrSkip(tx, key, value, expiry_millis)
	if err == nil {
		if updated {
			return newPgRedisString("OK")
		} else {
			return newPgRedisNil()
		}
	} else {
		log.Println("ERROR: ", err.Error())
		return newPgRedisNil()
	}
}

type strlenCommand struct{}

func (cmd *strlenCommand) Execute(command *redisproto.Command, redis *PgRedis, tx *sql.Tx) pgRedisValue {
	success, resp, err := redis.strings.Get(tx, command.Get(1))
	if success {
		return newPgRedisInt(int64(len(resp.Value)))
	} else if !success && err == nil {
		return newPgRedisInt(0)
	} else {
		return newPgRedisError(err.Error())
	}
}

func commandExValueInMillis(command *redisproto.Command) int {
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

func commandPxValueInMillis(command *redisproto.Command) int {
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

func indexOfValue(command *redisproto.Command, value string) int {
	for i := 1; i < command.ArgCount(); i++ {
		if string(command.Get(i)) == value {
			return i
		}
	}
	return 0
}

func commandHasValue(command *redisproto.Command, value string) bool {
	for i := 1; i < command.ArgCount(); i++ {
		if string(command.Get(i)) == value {
			return true
		}
	}
	return false
}
