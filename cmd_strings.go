package pgredis

import (
	"bytes"
	"log"
	"strconv"

	"github.com/32bitkid/bitreader"
	"github.com/secmask/go-redisproto"
)

type appendCommand struct{}

func (cmd *appendCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	value := command.Get(2)
	newValue, err := insertOrAppendString(key, value, redis.db)
	if err == nil {
		return writer.WriteInt(int64(len(newValue)))
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

type bitcountCommand struct{}

func (cmd *bitcountCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	success, resp, err := getString(command.Get(1), redis.db)

	if success {
		byteReader := bytes.NewReader(resp.value)
		bitReader := bitreader.NewReader(byteReader)
		setCount := int64(0)
		checkedCount := int64(0)

		for {
			checkedCount += 1
			bitSet, bitErr := bitReader.Read1()
			if bitErr != nil {
				// this is never true, see https://github.com/32bitkid/bitreader/pull/2
				break
			}
			if bitSet {
				setCount += 1
			}
			if checkedCount >= int64(len(resp.value)) * 8 {
				break
			}
		}
		return writer.WriteInt(setCount)
	} else if !success && err == nil {
		return writer.WriteInt(0) // assumed to be an empty string, with all 0 bits
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteInt(0) // TODO probbly not right
	}
}

type getCommand struct{}

func (cmd *getCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	success, resp, err := getString(command.Get(1), redis.db)
	if success {
		return writer.WriteBulkString(string(resp.value))
	} else if !success && err == nil {
		return writer.WriteBulk(nil)
	} else {
		panic(err) // TODO ergh
	}
}

type getbitCommand struct{}

func (cmd *getbitCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	success, resp, err := getString(command.Get(1), redis.db)
	bitPosition, _ := strconv.Atoi(string(command.Get(2)))

	if success {
		byteReader := bytes.NewReader(resp.value)
		bitReader := bitreader.NewReader(byteReader)
		bitReader.Skip(uint(bitPosition))
		bitSet, err := bitReader.Read1()
		if err != nil {
			log.Println("ERROR: ", err.Error())
			return writer.WriteInt(0) // assumed to be an empty string, with all 0 bits
		}
		if bitSet {
			return writer.WriteInt(1)
		} else {
			return writer.WriteInt(0)
		}
	} else if !success && err == nil {
		return writer.WriteInt(0) // assumed to be an empty string, with all 0 bits
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteInt(0) // TODO probbly not right
	}
}

type getsetCommand struct{}

func (cmd *getsetCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	expiry_millis := 0
	getSuccess, resp, err := getString(command.Get(1), redis.db)

	insertErr := insertOrUpdateString(command.Get(1), command.Get(2), expiry_millis, redis.db)
	if insertErr == nil {
		if getSuccess {
			return writer.WriteBulkString(string(resp.value))
		} else if !getSuccess && err == nil {
			return writer.WriteBulk(nil)
		} else {
			panic(err) // TODO ergh
		}
	} else {
		log.Println("DB ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

type getrangeCommand struct{}

func (cmd *getrangeCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	success, result, err := getString(command.Get(1), redis.db)
	if success {
		start, _ := strconv.Atoi(string(command.Get(2)))
		end, _ := strconv.Atoi(string(command.Get(3)))

		if start < 0 {
			start = len(result.value) + start
		}

		if end < 0 {
			end = len(result.value) + end
		}

		end += 1

		if end < start {
			end = start
		}

		if start > len(result.value) {
			start = len(result.value)
		}

		if end > len(result.value) {
			end = len(result.value)
		}

		return writer.WriteBulkString(string(result.value[start:end]))
	} else if !success && err == nil {
		return writer.WriteBulk(nil)
	} else {
		return err
	}
}

type setCommand struct{}

func (cmd *setCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
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
		updated, err := updateOrSkipString(command.Get(1), command.Get(2), expiry_millis, redis.db)
		if err == nil {
			if updated {
				return writer.WriteBulkString("OK")
			} else {
				return writer.WriteBulk(nil)
			}
		} else {
			log.Println("ERROR: ", err.Error())
			return writer.WriteBulk(nil)
		}
	} else if nxArgProvided { // only set the key if it doesn't already exists
		updated, err := insertOrSkipString(command.Get(1), command.Get(2), expiry_millis, redis.db)
		if err == nil {
			if updated {
				return writer.WriteBulkString("OK")
			} else {
				return writer.WriteBulk(nil)
			}
		} else {
			log.Println("ERROR: ", err.Error())
			return writer.WriteBulk(nil)
		}
	} else {
		err := insertOrUpdateString(command.Get(1), command.Get(2), expiry_millis, redis.db)
		if err == nil {
			return writer.WriteBulkString("OK")
		} else {
			log.Println("ERROR: ", err.Error())
			return writer.WriteBulk(nil)
		}
	}
}

type setexCommand struct{}

func (cmd *setexCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	expiry_secs, _ := strconv.Atoi(string(command.Get(2)))
	value := command.Get(3)
	expiry_millis := expiry_secs * 1000

	err := insertOrUpdateString(key, value, expiry_millis, redis.db)
	if err == nil {
		return writer.WriteBulkString("OK")
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

type psetexCommand struct{}

func (cmd *psetexCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	expiry_millis, _ := strconv.Atoi(string(command.Get(2)))
	value := command.Get(3)
	err := insertOrUpdateString(key, value, expiry_millis, redis.db)
	if err == nil {
		return writer.WriteBulkString("OK")
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

type setnxCommand struct{}

func (cmd *setnxCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	value := command.Get(2)
	expiry_millis := 0

	updated, err := insertOrSkipString(key, value, expiry_millis, redis.db)
	if err == nil {
		if updated {
			return writer.WriteBulkString("OK")
		} else {
			return writer.WriteBulk(nil)
		}
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

type strlenCommand struct{}

func (cmd *strlenCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	success, resp, err := getString(command.Get(1), redis.db)
	if success {
		return writer.WriteInt(int64(len(resp.value)))
	} else if !success && err == nil {
		return writer.WriteInt(0)
	} else {
		panic(err) // TODO ergh
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
