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
	keys       *repositories.KeyRepository
	strings    *repositories.StringRepository
	lists      *repositories.ListRepository
	sets       *repositories.SetRepository
	sortedsets *repositories.SortedSetRepository
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
		keys:       repositories.NewKeyRepository(db),
		strings:    repositories.NewStringRepository(db),
		lists:      repositories.NewListRepository(db),
		sets:       repositories.NewSetRepository(db),
		sortedsets: repositories.NewSortedSetRepository(db),
		commands: map[string]redisCommand{
			"APPEND":      &appendCommand{},
			"BITCOUNT":    &bitcountCommand{},
			"DECR":        &decrCommand{},
			"DEL":         &delCommand{},
			"DECRBY":      &decrbyCommand{},
			"ECHO":        &echoCommand{},
			"EXISTS":      &existsCommand{},
			"EXPIRE":      &expireCommand{},
			"GET":         &getCommand{},
			"GETBIT":      &getbitCommand{},
			"GETRANGE":    &getrangeCommand{},
			"GETSET":      &getsetCommand{},
			"INCR":        &incrCommand{},
			"INCRBY":      &incrbyCommand{},
			"INCRBYFLOAT": &incrbyfloatCommand{},
			"LLEN":        &llenCommand{},
			"LPUSH":       &lpushCommand{},
			"LRANGE":      &lrangeCommand{},
			"MGET":        &mgetCommand{},
			"MSET":        &msetCommand{},
			"PING":        &pingCommand{},
			"PSETEX":      &psetexCommand{},
			"QUIT":        &quitCommand{},
			"RPUSH":       &rpushCommand{},
			"SADD":        &saddCommand{},
			"SCARD":       &scardCommand{},
			"SET":         &setCommand{},
			"SETEX":       &setexCommand{},
			"SETNX":       &setnxCommand{},
			"SMEMBERS":    &smembersCommand{},
			"SREM":        &sremCommand{},
			"STRLEN":      &strlenCommand{},
			"TTL":         &ttlCommand{},
			"TYPE":        &typeCommand{},
			"FLUSHALL":    &flushallCommand{},
			"ZADD":        &zaddCommand{},
			"ZCARD":       &zcardCommand{},
			"ZRANGE":      &zrangeCommand{},
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

	return nil
}

func (redis *PgRedis) selectCmd(cmdString string) redisCommand {
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
		}
		cmdString := strings.ToUpper(string(command.Get(0)))
		cmd := redis.selectCmd(cmdString)
		ew = cmd.Execute(command, redis, writer)
		if command.IsLast() {
			writer.Flush()
		}
		if cmdString == "QUIT" {
			break
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
