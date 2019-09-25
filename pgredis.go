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
	db         *sql.DB
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
		hashes:     repositories.NewHashRepository(),
		keys:       repositories.NewKeyRepository(),
		strings:    repositories.NewStringRepository(),
		lists:      repositories.NewListRepository(),
		sets:       repositories.NewSetRepository(),
		sortedsets: repositories.NewSortedSetRepository(),
		db:         db,
		commands: map[string]redisCommand{
			"APPEND":           &appendCommand{},
			"BITCOUNT":         &bitcountCommand{},
			"BRPOP":            &brpopCommand{},
			"DECR":             &decrCommand{},
			"DEL":              &delCommand{},
			"DECRBY":           &decrbyCommand{},
			"ECHO":             &echoCommand{},
			"EXISTS":           &existsCommand{},
			"EXPIRE":           &expireCommand{},
			"GET":              &getCommand{},
			"GETBIT":           &getbitCommand{},
			"GETRANGE":         &getrangeCommand{},
			"GETSET":           &getsetCommand{},
			"HGET":             &hgetCommand{},
			"HGETALL":          &hgetallCommand{},
			"HMGET":            &hmgetCommand{},
			"HMSET":            &hmsetCommand{},
			"HSET":             &hsetCommand{},
			"INCR":             &incrCommand{},
			"INFO":             &infoCommand{},
			"INCRBY":           &incrbyCommand{},
			"INCRBYFLOAT":      &incrbyfloatCommand{},
			"LLEN":             &llenCommand{},
			"LPOP":             &lpopCommand{},
			"LPUSH":            &lpushCommand{},
			"LRANGE":           &lrangeCommand{},
			"LREM":             &lremCommand{},
			"MGET":             &mgetCommand{},
			"MSET":             &msetCommand{},
			"PING":             &pingCommand{},
			"PSETEX":           &psetexCommand{},
			"PTTL":             &pttlCommand{},
			"QUIT":             &quitCommand{},
			"RPOP":             &rpopCommand{},
			"RPUSH":            &rpushCommand{},
			"SADD":             &saddCommand{},
			"SCARD":            &scardCommand{},
			"SSCAN":            &sscanCommand{},
			"SET":              &setCommand{},
			"SETEX":            &setexCommand{},
			"SETNX":            &setnxCommand{},
			"SMEMBERS":         &smembersCommand{},
			"SREM":             &sremCommand{},
			"STRLEN":           &strlenCommand{},
			"TTL":              &ttlCommand{},
			"TYPE":             &typeCommand{},
			"FLUSHALL":         &flushallCommand{},
			"ZADD":             &zaddCommand{},
			"ZCARD":            &zcardCommand{},
			"ZRANGE":           &zrangeCommand{},
			"ZRANGEBYSCORE":    &zrangebyscoreCommand{},
			"ZREVRANGE":        &zrevrangeCommand{},
			"ZREM":             &zremCommand{},
			"ZREMRANGEBYRANK":  &zremrangebyrankCommand{},
			"ZREMRANGEBYSCORE": &zremrangebyscoreCommand{},
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
	var tx *sql.Tx
	var txerr error
	var multiResponses = []pgRedisValue{}
	mode := "single"
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
		if cmdString == "MULTI" {
			log.Printf("command=MULTI\n")
			mode = "multi"
			multiResponses = make([]pgRedisValue, 0)

			// start a db transaction
			tx, txerr = redis.db.Begin()
			if txerr != nil {
				ew = writer.WriteError(txerr.Error())
			}
			_, err = tx.Exec("SET statement_timeout = 5000")
			if err != nil {
				log.Printf("Error setting statement timeout")
				break
			}
			_, err = tx.Exec("SET lock_timeout = 5000")
			if err != nil {
				log.Printf("Error setting lock timeout")
				break
			}
			log.Printf("opened MULTi transaction, sending OK\n")
			writer.WriteSimpleString("OK")
			writer.Flush()
		}
		if mode == "single" {
			cmd := redis.selectCmd(cmdString)

			// start a db transaction
			tx, txerr := redis.db.Begin()
			if txerr != nil {
				ew = writer.WriteError(txerr.Error())
			}
			_, err = tx.Exec("SET statement_timeout = 5000")
			if err != nil {
				log.Printf("Error setting statement timeout")
				break
			}
			_, err = tx.Exec("SET lock_timeout = 5000")
			if err != nil {
				log.Printf("Error setting lock timeout")
				break
			}

			result, err := cmd.Execute(command, redis, tx)
			if err != nil {
				log.Print("ERROR: %s", err.Error())
				newPgRedisError(err.Error()).writeTo(buffer)
				break
			}
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
		} else {
			log.Printf("MULTI command execution: %s\n", cmdString)
			if cmdString != "MULTI" && cmdString != "EXEC" && cmdString != "DISCARD" {
				cmd := redis.selectCmd(cmdString)
				result, err := cmd.Execute(command, redis, tx)
				if err != nil {
					log.Print("ERROR: %s", err.Error())
					newPgRedisError(err.Error()).writeTo(buffer)
					break
				}
				multiResponses = append(multiResponses, result)
				writer.WriteSimpleString("QUEUED")
				writer.Flush()
			}
		}

		if cmdString == "DISCARD" {
			if mode == "multi" {
				txerr = tx.Rollback()
				if txerr != nil {
					ew = writer.WriteError(txerr.Error())
				}
				writer.WriteSimpleString("OK")
				writer.Flush()
				multiResponses = make([]pgRedisValue, 0)
				mode = "single"
			} else {
				ew = writer.WriteError("DISCARD without MULTI")
			}
		} else if cmdString == "EXEC" {
			if mode == "multi" {
				log.Printf("about to process EXEC\n")
				txerr = tx.Commit()
				if txerr != nil {
					ew = writer.WriteError(txerr.Error())
				}
				redisArray := newPgRedisArray(multiResponses)
				foo := redisArray.writeTo(buffer)
				if foo != nil {
					log.Printf("serialisation error\n", foo)
				}
				buffer.Flush()
				multiResponses = make([]pgRedisValue, 0)
				mode = "single"
			} else {
				ew = writer.WriteError("EXEC without MULTI")
			}
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

func printDbStats(db *sql.DB) {
	stats := db.Stats()
	log.Printf("Database connection open with %d max connections", stats.MaxOpenConnections)
}
