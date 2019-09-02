package pgredis

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"pgredis/internal/repositories"

	_ "github.com/lib/pq"
	"github.com/secmask/go-redisproto"
)

type PgRedis struct {
	commands map[string]redisCommand
	strings  *repositories.StringRepository
}

func NewPgRedis(connStr string) *PgRedis {
	fmt.Println("Connecting to: ", connStr)
	db, err := openDatabaseWithRetries(connStr, 3)

	if err != nil {
		panic(err)
	}

	err = setupSchema(db)
	if err != nil {
		panic(err)
	}

	return &PgRedis{
		strings: repositories.NewStringRepository(db),
		commands: map[string]redisCommand{
			"APPEND":      &appendCommand{},
			"BITCOUNT":    &bitcountCommand{},
			"DECR":        &decrCommand{},
			"DECRBY":      &decrbyCommand{},
			"ECHO":        &echoCommand{},
			"GET":         &getCommand{},
			"GETBIT":      &getbitCommand{},
			"GETRANGE":    &getrangeCommand{},
			"GETSET":      &getsetCommand{},
			"INCR":        &incrCommand{},
			"INCRBY":      &incrbyCommand{},
			"INCRBYFLOAT": &incrbyfloatCommand{},
			"MGET":        &mgetCommand{},
			"PING":        &pingCommand{},
			"PSETEX":      &psetexCommand{},
			"SET":         &setCommand{},
			"SETEX":       &setexCommand{},
			"SETNX":       &setnxCommand{},
			"STRLEN":      &strlenCommand{},
			"TTL":         &ttlCommand{},
			"FLUSHALL":    &flushallCommand{},
		},
	}
}

func openDatabaseWithRetries(connStr string, retries int) (*sql.DB, error) {

	db, err := sql.Open("postgres", connStr)

	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		if retries > 0 {
			time.Sleep(3 * time.Second)

			return openDatabaseWithRetries(connStr, retries-1)
		} else {
			return nil, err
		}
	}
	return db, nil
}

func (redis *PgRedis) StartServer(bindAddress string, port string) error {
	listener, err := net.Listen("tcp", bindAddress+":"+port)
	if err != nil {
		panic(err)
	}
	log.Printf("pgredis started on " + bindAddress + ":" + port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error on accept: ", err)
			continue
		}
		go redis.handleConnection(conn)
	}
}

func setupSchema(db *sql.DB) error {
	_, err := db.Query("create table if not exists redisdata (key bytea PRIMARY KEY, value bytea not null, expires_at timestamp with time zone NULL)")
	if err != nil {
		return err
	}

	return nil
}

func (redis *PgRedis) selectCmd(data []byte) redisCommand {
	cmdString := strings.ToUpper(string(data))
	implementation := redis.commands[cmdString]
	if implementation == nil {
		implementation = &unrecognisedCommand{}
	}
	return implementation
}

func (redis *PgRedis) handleConnection(conn net.Conn) {
	defer conn.Close()
	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(bufio.NewWriter(conn))
	var ew error
	for {
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*redisproto.ProtocolError)
			if ok {
				ew = writer.WriteError(err.Error())
			} else {
				log.Println(err, " closed connection to ", conn.RemoteAddr())
				break
			}
		} else {
			cmd := redis.selectCmd(command.Get(0))
			ew = cmd.Execute(command, redis, writer)
		}
		if command.IsLast() {
			writer.Flush()
		}
		if ew != nil {
			log.Println("Connection closed", ew)
			break
		}
	}
}

func (redis *PgRedis) flushAll() error {
	err := redis.strings.FlushAll()
	if err != nil {
		return err
	}
	return nil
}
