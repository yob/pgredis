package pgredis

import (
	"log"
	"strconv"

	"github.com/secmask/go-redisproto"
)

type zaddCommand struct{}

func (cmd *zaddCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
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

	log.Printf("xxArgProvided: %v", xxArgProvided)
	log.Printf("nxArgProvided: %v", nxArgProvided)
	log.Printf("incrArgProvided: %v", incrArgProvided)
	log.Printf("values: %v", values)

	updated, err := redis.sortedsets.Add(key, values, chArgProvided)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	} else {
		return writer.WriteInt(updated)
	}
}

type zcardCommand struct{}

func (cmd *zcardCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)

	count, err := redis.sortedsets.Cardinality(key)
	if err != nil {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	} else {
		return writer.WriteInt(count)
	}
}

type zrangeCommand struct{}

func (cmd *zrangeCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	start, _ := strconv.Atoi(string(command.Get(2)))
	end, _ := strconv.Atoi(string(command.Get(3)))
	includeScores := string(command.Get(4)) == "WITHSCORES"

	items, err := redis.sortedsets.Range(key, start, end, includeScores)
	if err == nil {
		return writer.WriteBulkStrings(items)
	} else {
		log.Println("ERROR: ", err.Error())
		return writer.WriteBulk(nil)
	}
}

type zremCommand struct{}

func (cmd *zremCommand) Execute(command *redisproto.Command, redis *PgRedis, writer *redisproto.Writer) error {
	key := command.Get(1)
	values := make([][]byte, 0)
	for i := 2; i < command.ArgCount(); i++ {
		values = append(values, command.Get(i))
	}

	updated, err := redis.sortedsets.Remove(key, values)

	if err != nil {
		log.Println("ERROR: ", err.Error())
		return writer.WriteError(err.Error())
	} else {
		return writer.WriteInt(updated)
	}
}
