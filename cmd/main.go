package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	_ "github.com/lib/pq"
	"github.com/secmask/go-redisproto"
	"github.com/urfave/cli"
)

const (
  host     = "db"
  port     = 5432
  user     = "pgredis"
  password = "fnord"
  dbname   = "pgredis"
)

func logf(msg string, args ...interface{}) {
	_, err := fmt.Fprintf(os.Stderr, msg+"\n", args...)

	if err != nil {
		panic("failed to write to stderr: " + err.Error())
	}
}

func main() {
	app := cli.NewApp()

	app.Name = "pgredis"
	app.Usage = "Redis in front, postgresql out back"
	app.Version = "dev"

	app.Commands = []cli.Command{
		{
			Name:  "server",
			Usage: "start the pgredis server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "bind, b",
					Usage: "IP address to listen on",
					Value: "0.0.0.0",
				},
				cli.StringFlag{
					Name:  "port, p",
					Usage: "the port to listen on",
					Value: "6379",
				},
			},
			Action: func(ctx *cli.Context) error {
				return startServer(ctx.String("bind"), ctx.String("port"))
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		logf("%+v", err)
		os.Exit(1)
	}
}

func startServer(bindAddress string, port string) error {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, host, dbname)
	fmt.Println("Connecting to: ", connStr)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = setupSchema(db)
	if err != nil {
		panic(err)
	}

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
		go handleConnection(conn, db)
	}
}

func setupSchema(db *sql.DB) error {
	_, err := db.Query("create table if not exists redisdata (key TEXT PRIMARY KEY, value TEXT not null)")
	if err != nil {
		return err
	}

	return nil
}

func handleConnection(conn net.Conn, db *sql.DB) {
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
			cmd := strings.ToUpper(string(command.Get(0)))
			switch cmd {
			case "GET":
				resp, err := getString(command.Get(1), db)
				if resp != nil {
					ew = writer.WriteBulkString(string(resp))
				} else if resp == nil && err == nil {
					ew = writer.WriteBulk(nil)
				} else {
					panic(err)
				}
			case "SET":
				err := setString(command.Get(1), command.Get(2), db)
				if err == nil {
					ew = writer.WriteBulkString("OK")
				} else {
					ew = writer.WriteBulk(nil)
				}
			default:
				ew = writer.WriteError("Command not support")
			}
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

func getString(key []byte, db *sql.DB) ([]byte, error) {
    var value []byte

	sqlStat := "SELECT value FROM redisdata WHERE key = $1"
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

func setString(key []byte, value []byte, db *sql.DB) error {
	sqlStat := "INSERT INTO redisdata(key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
	_, err := db.Exec(sqlStat, key, value)
	if err != nil {
		return err
	}
	return nil
}
