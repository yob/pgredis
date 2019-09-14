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
	db *sql.DB
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
		db:			db,
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

func (redis *PgRedis) handleConnection(conn net.Conn) {
	defer conn.Close()
	parser := redisproto.NewParser(conn)
	buffer := bufio.NewWriter(conn)
	writer := redisproto.NewWriter(buffer)
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

		// start a db transaction
		tx, txerr := redis.db.Begin()
		if txerr != nil {
			ew = writer.WriteError(txerr.Error())
		}

		result := cmd.Execute(command, redis, tx)
		ew = result.writeTo(buffer)
		if ew != nil {
			// this should be rare, there's no much that can go wrong when writing to an in memory buffer
			log.Println("Error during command execution, connection closed", ew)
			break
		}

		buffer.Flush()

		txerr = tx.Commit()
		if txerr != nil {
			ew = writer.WriteError(txerr.Error())
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
