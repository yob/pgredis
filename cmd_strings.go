package pgredis

import (
	"log"
	"strconv"

	"github.com/secmask/go-redisproto"
)

type getCommand struct{}

func (cmd *getCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	resp, err := getString(command.Get(1), redis.db)
	if resp != nil {
		return writer.WriteBulkString(string(resp))
	} else if resp == nil && err == nil {
		return writer.WriteBulk(nil)
	} else {
		panic(err) // TODO ergh
	}
}

type getrangeCommand struct{}

func (cmd *getrangeCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	data, err := getString(command.Get(1), redis.db)
	if data != nil {
		start, _ := strconv.Atoi(string(command.Get(2)))
		end, _ := strconv.Atoi(string(command.Get(3)))

		if start < 0 {
			start = len(data) + start
		}

		if end < 0 {
			end = len(data) + end
		}

		end += 1

		if end < start {
			end = start
		}

		if start > len(data) {
			start = len(data)
		}

		if end > len(data) {
			end = len(data)
		}

		return writer.WriteBulkString(string(data[start:end]))
	} else if data == nil && err == nil {
		return writer.WriteBulk(nil)
	} else {
		return err
	}
}

type setCommand struct{}

func (cmd *setCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	exValue := commandExValue(command)
	pxValue := commandPxValue(command)
	if exValue > 0 {
		return setEx(command.Get(1), command.Get(2), exValue, redis, writer)
	} else if pxValue > 0 {
		return setPx(command.Get(1), command.Get(2), pxValue, redis, writer)
	} else {
		return setPlain(command.Get(1), command.Get(2), redis, writer)
	}

}

type setexCommand struct{}

func (cmd *setexCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	expiry_secs_int, _ := strconv.Atoi(string(command.Get(2)))
	return setEx(command.Get(1), command.Get(3), expiry_secs_int, redis, writer)
}

type psetexCommand struct{}

func (cmd *psetexCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	expiry_millis_int, _ := strconv.Atoi(string(command.Get(2)))
	return setPx(command.Get(1), command.Get(3), expiry_millis_int, redis, writer)
}

func setPlain(key []byte, value []byte, redis *PgRedis, writer *redisproto.Writer) error {
	err := insertOrUpdateString(key, value, 0, redis.db)
	if err == nil {
		return writer.WriteBulkString("OK")
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

func setEx(key []byte, value []byte, expiry_secs int, redis *PgRedis, writer *redisproto.Writer) error {
	expiry_millis := expiry_secs * 1000
	err := insertOrUpdateString(key, value, expiry_millis, redis.db)
	if err == nil {
		return writer.WriteBulkString("OK")
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

func setPx(key []byte, value []byte, expiry_millis int, redis *PgRedis, writer *redisproto.Writer) error {
	err := insertOrUpdateString(key, value, expiry_millis, redis.db)
	if err == nil {
		return writer.WriteBulkString("OK")
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

func commandExValue(command *redisproto.Command) int {
	indexOfEx := indexOfValue(command, "EX")
	if indexOfEx == 0 {
		return 0
	} else {
		value, err := strconv.Atoi(string(command.Get(indexOfEx + 1)))
		if err == nil {
			return value
		} else {
			return 0
		}
	}
}

func commandPxValue(command *redisproto.Command) int {
	indexOfPx := indexOfValue(command, "PX")
	if indexOfPx == 0 {
		return 0
	} else {
		value, err := strconv.Atoi(string(command.Get(indexOfPx + 1)))
		if err == nil {
			return value
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
