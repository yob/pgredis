package pgredis

import (
//	"bytes"
	"log"
	"strconv"
//
//	"github.com/32bitkid/bitreader"
	"github.com/secmask/go-redisproto"
)
//
//type appendCommand struct{}
//
//func (cmd *appendCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	key := command.Get(1)
//	value := command.Get(2)
//	newValue, err := redis.strings.InsertOrAppend(key, value)
//	if err == nil {
//		return writer.WriteInt(int64(len(newValue)))
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteBulk(nil)
//	}
//}
//
//type bitcountCommand struct{}
//
//func intOrZero(value string) int {
//	if value == "" {
//		return 0
//	} else {
//		result, err := strconv.Atoi(value)
//		if err != nil {
//			return 0
//		}
//		return result
//	}
//}
//
//func (cmd *bitcountCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	key := command.Get(1)
//	success, result, err := redis.strings.Get(key)
//
//	if success {
//		start, _ := strconv.Atoi(string(command.Get(2)))
//		end, _ := strconv.Atoi(string(command.Get(3)))
//
//		if start < 0 {
//			start = len(result.Value) + start
//		}
//
//		if end < 0 {
//			end = len(result.Value) + end
//		}
//
//		end += 1
//
//		if end < start {
//			end = start
//		}
//
//		if start > len(result.Value) {
//			start = len(result.Value)
//		}
//
//		if end > len(result.Value) {
//			end = len(result.Value)
//		}
//		bitsToRead := (end - start) * 8
//
//		byteReader := bytes.NewReader(result.Value)
//		bitReader := bitreader.NewReader(byteReader)
//		bitReader.Skip(uint(start))
//		setCount := int64(0)
//
//		for i := 0; i < bitsToRead; i++ {
//			bitSet, bitErr := bitReader.Read1()
//			if bitErr != nil {
//				break
//			}
//			if bitSet {
//				setCount += 1
//			}
//		}
//		return writer.WriteInt(setCount)
//	} else if !success && err == nil {
//		return writer.WriteInt(0) // assumed to be an empty string, with all 0 bits
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteInt(0) // TODO probbly not right
//	}
//}
//
//type decrCommand struct{}
//
//func (cmd *decrCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	key := command.Get(1)
//	newValue, err := redis.strings.Incr(key, -1)
//	if err == nil {
//		intValue, _ := strconv.Atoi(string(newValue))
//		return writer.WriteInt(int64(intValue))
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteBulk(nil)
//	}
//}
//
//type decrbyCommand struct{}
//
//func (cmd *decrbyCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	key := command.Get(1)
//	by, _ := strconv.Atoi(string(command.Get(2)))
//	newValue, err := redis.strings.Incr(key, by*-1)
//	if err == nil {
//		intValue, _ := strconv.Atoi(string(newValue))
//		return writer.WriteInt(int64(intValue))
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteBulk(nil)
//	}
//}
//
type getCommand struct{}

func (cmd *getCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	success, resp, err := redis.strings.Get(command.Get(1))
	if success {
		return writer.WriteBulkString(string(resp.Value))
	} else if !success && err == nil {
		return writer.WriteBulk(nil)
	} else {
		panic(err) // TODO ergh
	}
}
//
//type getbitCommand struct{}
//
//func (cmd *getbitCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	success, resp, err := redis.strings.Get(command.Get(1))
//	bitPosition, _ := strconv.Atoi(string(command.Get(2)))
//
//	if success {
//		byteReader := bytes.NewReader(resp.Value)
//		bitReader := bitreader.NewReader(byteReader)
//		bitReader.Skip(uint(bitPosition))
//		bitSet, err := bitReader.Read1()
//		if err != nil {
//			log.Println("ERROR: ", err.Error())
//			return writer.WriteInt(0) // assumed to be an empty string, with all 0 bits
//		}
//		if bitSet {
//			return writer.WriteInt(1)
//		} else {
//			return writer.WriteInt(0)
//		}
//	} else if !success && err == nil {
//		return writer.WriteInt(0) // assumed to be an empty string, with all 0 bits
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteInt(0) // TODO probbly not right
//	}
//}
//
//type getsetCommand struct{}
//
//func (cmd *getsetCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	expiry_millis := 0
//	getSuccess, resp, err := redis.strings.Get(command.Get(1))
//
//	insertErr := redis.strings.InsertOrUpdate(command.Get(1), command.Get(2), expiry_millis)
//	if insertErr == nil {
//		if getSuccess {
//			return writer.WriteBulkString(string(resp.Value))
//		} else if !getSuccess && err == nil {
//			return writer.WriteBulk(nil)
//		} else {
//			panic(err) // TODO ergh
//		}
//	} else {
//		log.Println("DB ERROR: ", err.Error())
//		return writer.WriteBulk(nil)
//	}
//}
//
//type getrangeCommand struct{}
//
//func (cmd *getrangeCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	success, result, err := redis.strings.Get(command.Get(1))
//	if success {
//		start, _ := strconv.Atoi(string(command.Get(2)))
//		end, _ := strconv.Atoi(string(command.Get(3)))
//
//		if start < 0 {
//			start = len(result.Value) + start
//		}
//
//		if end < 0 {
//			end = len(result.Value) + end
//		}
//
//		end += 1
//
//		if end < start {
//			end = start
//		}
//
//		if start > len(result.Value) {
//			start = len(result.Value)
//		}
//
//		if end > len(result.Value) {
//			end = len(result.Value)
//		}
//
//		return writer.WriteBulkString(string(result.Value[start:end]))
//	} else if !success && err == nil {
//		return writer.WriteBulk(nil)
//	} else {
//		return err
//	}
//}
//
type incrCommand struct{}

func (cmd *incrCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	newValue, err := redis.strings.Incr(key, 1)
	if err == nil {
		intValue, _ := strconv.Atoi(string(newValue))
		return writer.WriteInt(int64(intValue))
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}
//
//type incrbyCommand struct{}
//
//func (cmd *incrbyCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	key := command.Get(1)
//	by, _ := strconv.Atoi(string(command.Get(2)))
//	newValue, err := redis.strings.Incr(key, by)
//	if err == nil {
//		intValue, _ := strconv.Atoi(string(newValue))
//		return writer.WriteInt(int64(intValue))
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteBulk(nil)
//	}
//}
//
//type incrbyfloatCommand struct{}
//
//func (cmd *incrbyfloatCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	key := command.Get(1)
//	by, _ := strconv.ParseFloat(string(command.Get(2)), 64)
//	newValue, err := redis.strings.IncrDecimal(key, by)
//	if err == nil {
//		return writer.WriteBulkString(string(newValue))
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteBulk(nil)
//	}
//}
//
//type mgetCommand struct{}
//
//func (cmd *mgetCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	result := make([]interface{}, command.ArgCount()-1)
//	for i := 1; i < command.ArgCount(); i++ {
//		// TODO calling getStrings in a loop like this returns the correct result, but is super
//		//      inefficient
//		success, resp, _ := redis.strings.Get(command.Get(i))
//		if success {
//			result[i-1] = string(resp.Value)
//		}
//	}
//	return writer.WriteObjectsSlice(result)
//}
//
//type msetCommand struct{}
//
//func (cmd *msetCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	// TODO Using string because I can't use byte slices as a map key, but this probably breaks
//	// some compatibility with redis
//	items := make(map[string]string)
//	for i := 1; i < command.ArgCount(); i += 2 {
//		items[string(command.Get(i))] = string(command.Get(i + 1))
//	}
//	log.Printf("items: %v\n", items)
//	err := redis.strings.InsertOrUpdateMultiple(items)
//	if err == nil {
//		return writer.WriteBulkString("OK")
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteBulk(nil)
//	}
//}
//
//type setCommand struct{}
//
//func (cmd *setCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	expiry_millis := 0
//	exValue := commandExValueInMillis(command)
//	if exValue > 0 {
//		expiry_millis = exValue
//	}
//	pxValue := commandPxValueInMillis(command)
//	if pxValue > 0 {
//		expiry_millis = pxValue
//	}
//
//	xxArgProvided := commandHasValue(command, "XX")
//	nxArgProvided := commandHasValue(command, "NX")
//	if xxArgProvided { // only set the key if it already exists
//		updated, err := redis.strings.UpdateOrSkip(command.Get(1), command.Get(2), expiry_millis)
//		if err == nil {
//			if updated {
//				return writer.WriteBulkString("OK")
//			} else {
//				return writer.WriteBulk(nil)
//			}
//		} else {
//			log.Println("ERROR: ", err.Error())
//			return writer.WriteBulk(nil)
//		}
//	} else if nxArgProvided { // only set the key if it doesn't already exists
//		updated, err := redis.strings.InsertOrSkip(command.Get(1), command.Get(2), expiry_millis)
//		if err == nil {
//			if updated {
//				return writer.WriteBulkString("OK")
//			} else {
//				return writer.WriteBulk(nil)
//			}
//		} else {
//			log.Println("ERROR: ", err.Error())
//			return writer.WriteBulk(nil)
//		}
//	} else {
//		err := redis.strings.InsertOrUpdate(command.Get(1), command.Get(2), expiry_millis)
//		if err == nil {
//			return writer.WriteBulkString("OK")
//		} else {
//			log.Println("ERROR: ", err.Error())
//			return writer.WriteBulk(nil)
//		}
//	}
//}
//
//type setexCommand struct{}
//
//func (cmd *setexCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	key := command.Get(1)
//	expiry_secs, _ := strconv.Atoi(string(command.Get(2)))
//	value := command.Get(3)
//	expiry_millis := expiry_secs * 1000
//
//	err := redis.strings.InsertOrUpdate(key, value, expiry_millis)
//	if err == nil {
//		return writer.WriteBulkString("OK")
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteBulk(nil)
//	}
//}
//
//type psetexCommand struct{}
//
//func (cmd *psetexCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	key := command.Get(1)
//	expiry_millis, _ := strconv.Atoi(string(command.Get(2)))
//	value := command.Get(3)
//	err := redis.strings.InsertOrUpdate(key, value, expiry_millis)
//	if err == nil {
//		return writer.WriteBulkString("OK")
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteBulk(nil)
//	}
//}
//
//type setnxCommand struct{}
//
//func (cmd *setnxCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	key := command.Get(1)
//	value := command.Get(2)
//	expiry_millis := 0
//
//	updated, err := redis.strings.InsertOrSkip(key, value, expiry_millis)
//	if err == nil {
//		if updated {
//			return writer.WriteBulkString("OK")
//		} else {
//			return writer.WriteBulk(nil)
//		}
//	} else {
//		log.Println("ERROR: ", err.Error())
//		return writer.WriteBulk(nil)
//	}
//}
//
//type strlenCommand struct{}
//
//func (cmd *strlenCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
//	success, resp, err := redis.strings.Get(command.Get(1))
//	if success {
//		return writer.WriteInt(int64(len(resp.Value)))
//	} else if !success && err == nil {
//		return writer.WriteInt(0)
//	} else {
//		panic(err) // TODO ergh
//	}
//}
//
//func commandExValueInMillis(command *redisproto.Command) int {
//	indexOfEx := indexOfValue(command, "EX")
//	if indexOfEx == 0 {
//		return 0
//	} else {
//		seconds, err := strconv.Atoi(string(command.Get(indexOfEx + 1)))
//		if err == nil {
//			return seconds * 1000
//		} else {
//			return 0
//		}
//	}
//}
//
//func commandPxValueInMillis(command *redisproto.Command) int {
//	indexOfPx := indexOfValue(command, "PX")
//	if indexOfPx == 0 {
//		return 0
//	} else {
//		millis, err := strconv.Atoi(string(command.Get(indexOfPx + 1)))
//		if err == nil {
//			return millis
//		} else {
//			return 0
//		}
//	}
//}
//
//func indexOfValue(command *redisproto.Command, value string) int {
//	for i := 1; i < command.ArgCount(); i++ {
//		if string(command.Get(i)) == value {
//			return i
//		}
//	}
//	return 0
//}
//
//func commandHasValue(command *redisproto.Command, value string) bool {
//	for i := 1; i < command.ArgCount(); i++ {
//		if string(command.Get(i)) == value {
//			return true
//		}
//	}
//	return false
//}
