package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/secmask/go-redisproto"
	"github.com/urfave/cli"
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
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
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
				ew = writer.WriteBulkString("dummy")
			case "SET":
				ew = writer.WriteBulkString("OK")
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
