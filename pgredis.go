package pgredis

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/secmask/go-redisproto"
)

type PgRedis struct {
	db       *sql.DB
	commands map[string]redisCommand
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
		db: db,
		commands: map[string]redisCommand{
			"GET":      &getCommand{},
			"GETRANGE": &getrangeCommand{},
			"PSETEX":   &psetexCommand{},
			"SET":      &setCommand{},
			"SETEX":    &setexCommand{},
			"FLUSHALL": &flushallCommand{},
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
	_, err := db.Query("create table if not exists redisdata (key TEXT PRIMARY KEY, value TEXT not null, expires_at timestamp with time zone NULL)")
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

func flushAll(db *sql.DB) error {
	sqlStat := "DELETE FROM redisdata"
	_, err := db.Exec(sqlStat)
	if err != nil {
		return err
	}
	return nil
}

func getString(key []byte, db *sql.DB) ([]byte, error) {
	var value []byte

	sqlStat := "SELECT value FROM redisdata WHERE key = $1 AND (expires_at > now() OR expires_at IS NULL)"
	row := db.QueryRow(sqlStat, key)

	switch err := row.Scan(&value); err {
	case sql.ErrNoRows:
		return nil, nil
	case nil:
		return value, nil
	default:
		return nil, err
	}
}

func insertOrUpdateString(key []byte, value []byte, expiry_millis int, db *sql.DB) (err error) {
	if expiry_millis == 0 {
		sqlStat := "INSERT INTO redisdata(key, value, expires_at) VALUES ($1, $2, NULL) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL"
		_, err = db.Exec(sqlStat, key, value)
	} else {
		sqlStat := "INSERT INTO redisdata(key, value, expires_at) VALUES ($1, $2, now() + cast($3 as interval)) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"
		interval := fmt.Sprintf("%d milliseconds", expiry_millis)
		_, err = db.Exec(sqlStat, key, value, interval)
	}
	if err != nil {
		return err
	}
	return nil
}

func updateOrSkipString(key []byte, value []byte, expiry_millis int, db *sql.DB) (updated bool, err error) {
	var res sql.Result
	if expiry_millis == 0 {
		sqlStat := "UPDATE redisdata SET value=$2, expires_at=NULL WHERE key=$1 AND (expires_at IS NULL OR expires_at < now())"
		res , err = db.Exec(sqlStat, key, value)
		count, _ := res.RowsAffected()
		updated = count > 0
	} else {
		sqlStat := "UPDATE redisdata SET value=$2, expires_at=now() + cast($3 as interval) WHERE key=$1 AND (expires_at IS NULL OR expires_at < now())"
		interval := fmt.Sprintf("%d milliseconds", expiry_millis)
		res, err = db.Exec(sqlStat, key, value, interval)
		count, _ := res.RowsAffected()
		updated = count > 0
	}
	if err != nil {
		return updated, err
	}
	return false, nil
}
