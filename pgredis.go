package pgredis

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/yob/pgredis/internal/repositories"

	_ "github.com/lib/pq"
	"github.com/secmask/go-redisproto"
)

const (
	MAX_COMMAND_QUEUE_SIZE = 100
)

type PgRedis struct {
	commands   map[string]redisCommand
	hashes     *repositories.HashRepository
	keys       *repositories.KeyRepository
	strings    *repositories.StringRepository
	lists      *repositories.ListRepository
	sets       *repositories.SetRepository
	sortedsets *repositories.SortedSetRepository
	connCount  uint64
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

	redisproto.MaxNumArg = 1024

	return &PgRedis{
		hashes:     repositories.NewHashRepository(),
		keys:       repositories.NewKeyRepository(),
		strings:    repositories.NewStringRepository(),
		lists:      repositories.NewListRepository(),
		sets:       repositories.NewSetRepository(),
		sortedsets: repositories.NewSortedSetRepository(),
		connCount:  0,
		db:         db,
		commands: map[string]redisCommand{
			"APPEND":           &appendCommand{},
			"BITCOUNT":         &bitcountCommand{},
			"BRPOP":            &brpopCommand{},
			"CLIENT":           &clientCommand{},
			"DBSIZE":           &dbsizeCommand{},
			"DECR":             &decrCommand{},
			"DEL":              &delCommand{},
			"DECRBY":           &decrbyCommand{},
			"ECHO":             &echoCommand{},
			"EXISTS":           &existsCommand{},
			"EXPIRE":           &expireCommand{},
			"FLUSHALL":         &flushallCommand{},
			"FLUSHDB":          &flushallCommand{},
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
			"SELECT":           &selectCommand{},
			"SET":              &setCommand{},
			"SETEX":            &setexCommand{},
			"SETNX":            &setnxCommand{},
			"SMEMBERS":         &smembersCommand{},
			"SREM":             &sremCommand{},
			"STRLEN":           &strlenCommand{},
			"TTL":              &ttlCommand{},
			"TYPE":             &typeCommand{},
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
	listener, err := net.Listen("tcp", net.JoinHostPort(bindAddress, strconv.Itoa(port)))
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
		atomic.AddUint64(&redis.connCount, 1)
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
	var requestQueue = []redisRequest{}
	for {
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*redisproto.ProtocolError)
			if ok {
				writer.WriteError(err.Error())
			} else {
				log.Println(err, " closed connection to ", conn.RemoteAddr())
				break
			}
		}
		requestQueue = append(requestQueue, newRequestFromRedisProto(command))

		lastRequest := requestQueue[len(requestQueue)-1]
		lastRequestCmd := lastRequest.CommandString()
		firstRequest := requestQueue[0]
		firstRequestCmd := firstRequest.CommandString()

		if len(requestQueue) > MAX_COMMAND_QUEUE_SIZE {
			writer.WriteError(fmt.Sprintf("Max command queue size (%d) exceeded", MAX_COMMAND_QUEUE_SIZE))
			writer.Flush()
			requestQueue = []redisRequest{}
		} else if lastRequestCmd == "DISCARD" {
			writer.WriteSimpleString("OK")
			writer.Flush()
			requestQueue = []redisRequest{}
		} else if len(requestQueue) == 1 && lastRequestCmd == "MULTI" {
			writer.WriteSimpleString("OK")
			writer.Flush()
		} else if lastRequestCmd == "EXEC" && firstRequestCmd != "MULTI" {
			writer.WriteError("EXEC without MULTI")
			writer.Flush()
			requestQueue = []redisRequest{}
		} else if len(requestQueue) == 1 {
			ok := redis.executeSingleCommand(requestQueue[0], buffer)
			writer.Flush()
			requestQueue = []redisRequest{}
			if !ok {
				break
			}
		} else if len(requestQueue) > 1 && firstRequestCmd == "MULTI" && lastRequestCmd != "EXEC" {
			writer.WriteSimpleString("QUEUED")
			writer.Flush()
		} else if len(requestQueue) > 1 && firstRequestCmd == "MULTI" && lastRequestCmd == "EXEC" {
			ok := redis.executeMultiCommand(requestQueue[1:len(requestQueue)-1], buffer)
			writer.Flush()
			requestQueue = []redisRequest{}
			if !ok {
				break
			}
		} else {
			writer.WriteError("Unrecognised command sequence")
			requestQueue = []redisRequest{}
		}
	}
	// ensure everything is written to the socket before we close it
	buffer.Flush()
}

func (redis *PgRedis) executeSingleCommand(request redisRequest, buffer *bufio.Writer) bool {
	// start a db transaction
	tx, txerr := redis.db.Begin()
	if txerr != nil {
		newPgRedisError(txerr.Error()).writeTo(buffer)
		return false
	}
	defer tx.Rollback()

	_, err := tx.Exec("SET statement_timeout = 5000")
	if err != nil {
		newPgRedisError("Error setting statement timeout").writeTo(buffer)
		return false
	}
	_, err = tx.Exec("SET lock_timeout = 5000")
	if err != nil {
		newPgRedisError("Error setting lock timeout").writeTo(buffer)
		return false
	}

	cmdObject := redis.selectCmd(request.CommandString())

	keysToLock := cmdObject.keysToLock(&request)
	err = redis.keys.LockKeys(tx, keysToLock)
	if err != nil {
		newPgRedisError("Error locking keys").writeTo(buffer)
		return false
	}

	result, err := cmdObject.Execute(&request, redis, tx)
	if err != nil {
		newPgRedisError(err.Error()).writeTo(buffer)
		return false
	}

	ew := result.writeTo(buffer)
	if ew != nil {
		// this should be rare, there's no much that can go wrong when writing to an in memory buffer
		newPgRedisError(fmt.Sprintf("Error during command execution, connection closed: %s", ew)).writeTo(buffer)

		// we may not be able to write to the client, so also log on the server
		log.Println("Error during command execution, connection closed", ew)
		return false
	}

	buffer.Flush()

	txerr = tx.Commit()
	if txerr != nil {
		newPgRedisError(txerr.Error()).writeTo(buffer)
		return false
	}

	return true
}

func (redis *PgRedis) executeMultiCommand(requestQueue []redisRequest, buffer *bufio.Writer) bool {
	log.Println("execute single command")

	// start a db transaction
	tx, txerr := redis.db.Begin()
	if txerr != nil {
		newPgRedisError(txerr.Error()).writeTo(buffer)
		return false
	}
	defer tx.Rollback()

	_, err := tx.Exec("SET statement_timeout = 5000")
	if err != nil {
		newPgRedisError("Error setting statement timeout").writeTo(buffer)
		return false
	}
	_, err = tx.Exec("SET lock_timeout = 5000")
	if err != nil {
		newPgRedisError("Error setting lock timeout").writeTo(buffer)
		return false
	}

	keysToLock := []string{}
	for _, nextRequest := range requestQueue {
		cmdObject := redis.selectCmd(nextRequest.CommandString())
		keysToLock = append(keysToLock, cmdObject.keysToLock(&nextRequest)...)
	}
	err = redis.keys.LockKeys(tx, keysToLock)
	if err != nil {
		newPgRedisError("Error locking keys").writeTo(buffer)
		return false
	}

	multiResponses := []pgRedisValue{}
	for _, nextRequest := range requestQueue {
		cmdObject := redis.selectCmd(nextRequest.CommandString())
		result, err := cmdObject.Execute(&nextRequest, redis, tx)
		if err != nil {
			newPgRedisError(err.Error()).writeTo(buffer)
			return false
		}
		multiResponses = append(multiResponses, result)

		buffer.Flush()
	}

	// a multi command was executed
	redisArray := newPgRedisArray(multiResponses)
	err = redisArray.writeTo(buffer)
	if err != nil {
		newPgRedisError(fmt.Sprintf("Error during command execution, connection closed: %s", err)).writeTo(buffer)

		// we may not be able to write to the client, so also log on the server
		log.Println("Error during command execution, connection closed", err)
		return false
	}

	txerr = tx.Commit()
	if txerr != nil {
		newPgRedisError(txerr.Error()).writeTo(buffer)
		return false
	}

	return true
}

func printDbStats(db *sql.DB) {
	stats := db.Stats()
	log.Printf("Database connection open with %d max connections", stats.MaxOpenConnections)
}
