package pgredis

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/yob/pgredis/internal/repositories"

	_ "github.com/lib/pq"
	"github.com/secmask/go-redisproto"
)

type PgRedis struct {
	commands   map[string]redisCommand
	hashes     *repositories.HashRepository
	keys       *repositories.KeyRepository
	strings    *repositories.StringRepository
	lists      *repositories.ListRepository
	sets       *repositories.SetRepository
	sortedsets *repositories.SortedSetRepository
}

// mirror the redisproto.Command interface
type redisProtoCommand interface {
    ArgCount() int
    Get(index int) []byte
    IsLast() bool
}

type localRedisCommand struct {
	args [][]byte
	last bool
}

func (cmd localRedisCommand) ArgCount() int {
	return len(cmd.args)
}

func (cmd localRedisCommand) Get(index int) []byte {
	return cmd.args[index]
}

func (cmd localRedisCommand) IsLast() bool {
	return cmd.last
}

func NewPgRedis(connStr string, maxConnections int) *PgRedis {
	fmt.Println("Connecting to: ", connStr)
	db, err := openDatabaseWithRetries(connStr, 3)

	if err != nil {
		panic(err)
	}

	db.SetMaxOpenConns(maxConnections)

	printDbStats(db)

	err = setupSchema(db)
	if err != nil {
		panic(err)
	}

	return &PgRedis{
		hashes:     repositories.NewHashRepository(db),
		keys:       repositories.NewKeyRepository(db),
		strings:    repositories.NewStringRepository(db),
		lists:      repositories.NewListRepository(db),
		sets:       repositories.NewSetRepository(db),
		sortedsets: repositories.NewSortedSetRepository(db),
		commands: map[string]redisCommand{
			//"APPEND":      &appendCommand{},
			//"BITCOUNT":    &bitcountCommand{},
			//"DECR":        &decrCommand{},
			//"DEL":         &delCommand{},
			//"DECRBY":      &decrbyCommand{},
			//"ECHO":        &echoCommand{},
			//"EXISTS":      &existsCommand{},
			//"EXPIRE":      &expireCommand{},
			"GET":         &getCommand{},
			//"GETBIT":      &getbitCommand{},
			//"GETRANGE":    &getrangeCommand{},
			//"GETSET":      &getsetCommand{},
			//"HGET":        &hgetCommand{},
			//"HGETALL":     &hgetallCommand{},
			//"HMGET":       &hmgetCommand{},
			//"HMSET":       &hmsetCommand{},
			//"HSET":        &hsetCommand{},
			"INCR":        &incrCommand{},
			//"INCRBY":      &incrbyCommand{},
			//"INCRBYFLOAT": &incrbyfloatCommand{},
			//"LLEN":        &llenCommand{},
			//"LPUSH":       &lpushCommand{},
			//"LRANGE":      &lrangeCommand{},
			//"MGET":        &mgetCommand{},
			//"MSET":        &msetCommand{},
			//"PING":        &pingCommand{},
			//"PSETEX":      &psetexCommand{},
			//"QUIT":        &quitCommand{},
			//"RPUSH":       &rpushCommand{},
			//"SADD":        &saddCommand{},
			//"SCARD":       &scardCommand{},
			//"SET":         &setCommand{},
			//"SETEX":       &setexCommand{},
			//"SETNX":       &setnxCommand{},
			//"SMEMBERS":    &smembersCommand{},
			//"SREM":        &sremCommand{},
			//"STRLEN":      &strlenCommand{},
			//"TTL":         &ttlCommand{},
			//"TYPE":        &typeCommand{},
			"FLUSHALL":    &flushallCommand{},
			//"ZADD":        &zaddCommand{},
			//"ZCARD":       &zcardCommand{},
			//"ZRANGE":      &zrangeCommand{},
			//"ZREM":        &zremCommand{},
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

func (redis *PgRedis) StartServer(bindAddress string, port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindAddress, port))
	if err != nil {
		panic(err)
	}
	log.Print(fmt.Sprintf("pgredis started on %s:%d", bindAddress, port))
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
	_, err := db.Query("create table if not exists redisdata (key bytea PRIMARY KEY, type bytea not null, value bytea not null, expires_at timestamp with time zone NULL)")
	if err != nil {
		return err
	}

	_, err = db.Query("create table if not exists redislists (key bytea, idx integer, value bytea not null, PRIMARY KEY(key, idx), FOREIGN KEY (key) REFERENCES redisdata (key) ON DELETE CASCADE);")
	if err != nil {
		return err
	}

	_, err = db.Query("create table if not exists redissets (key bytea, value bytea not null, PRIMARY KEY(key, value), FOREIGN KEY (key) REFERENCES redisdata (key) ON DELETE CASCADE);")
	if err != nil {
		return err
	}

	_, err = db.Query("create table if not exists rediszsets (key bytea, value bytea not null, score decimal not null, PRIMARY KEY(key, value), FOREIGN KEY (key) REFERENCES redisdata (key) ON DELETE CASCADE);")
	if err != nil {
		return err
	}

	_, err = db.Query("create table if not exists redishashes (key bytea, field bytea not null, value bytea not null, PRIMARY KEY(key, field), FOREIGN KEY (key) REFERENCES redisdata (key) ON DELETE CASCADE);")
	if err != nil {
		return err
	}

	return nil
}

func (redis *PgRedis) selectCmd(cmdString string) redisCommand {
	implementation := redis.commands[cmdString]
	if implementation == nil {
		implementation = &unrecognisedCommand{}
	}
	return implementation
}

func cloneCommand(cmd *redisproto.Command) redisProtoCommand {
	newCmd := localRedisCommand{}
	newCmd.last = cmd.IsLast()
	newCmd.args = make([][]byte, 0)
	for i := 0; i < cmd.ArgCount(); i++ {
		origSlice := cmd.Get(i)
		copySlice := append(make([]byte, 0, len(origSlice)), origSlice...)
		newCmd.args = append(newCmd.args, copySlice)
	}
	return newCmd
}

func (redis *PgRedis) handleConnection(conn net.Conn) {
	defer conn.Close()
	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(bufio.NewWriter(conn))
	mode := "single"
	queued_commands := []redisProtoCommand{}

	var ew error
	for {
		commandPtr, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*redisproto.ProtocolError)
			if ok {
				ew = writer.WriteError(err.Error())
			} else {
				log.Println(err, " closed connection to ", conn.RemoteAddr())
				break
			}
		}
		command := cloneCommand(commandPtr)
		log.Printf("command %v", command)
		cmdString := strings.ToUpper(string(command.Get(0)))
		if cmdString == "QUIT" {
			writer.WriteBulkString("OK")
			break
		} else if cmdString == "MULTI" {
			mode = "multi"
			writer.WriteBulkString("OK")
		} else if cmdString == "DISCARD" {
			writer.WriteBulkString("OK")
			queued_commands = []redisProtoCommand{}
			mode = "single"
		} else {
			if cmdString != "EXEC" {
				queued_commands = append(queued_commands, command)
			}

			if mode == "multi" && cmdString != "EXEC" {
				writer.WriteBulkString("QUEUED")
			}
			if mode == "single" || cmdString == "EXEC" {
				log.Printf("about to loop (because %s), queue %v", cmdString, queued_commands)
				for _, cmdToRun := range queued_commands {
					cmdToRunString := strings.ToUpper(string(cmdToRun.Get(0)))
					log.Printf("executing %s %v", cmdToRunString, cmdToRun)
					cmd := redis.selectCmd(cmdToRunString)
					ew = cmd.Execute(cmdToRun, redis, writer)
				}
				queued_commands = []redisProtoCommand{}
			}

			if cmdString == "EXEC" {
				writer.WriteBulkString("OK")
				mode = "single"
			}
		}
		log.Printf("mode: %s, queued: %v", mode, queued_commands)

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

func printDbStats(db *sql.DB) {
	stats := db.Stats()
	log.Printf("Database connection open with %d max connections", stats.MaxOpenConnections)
}
