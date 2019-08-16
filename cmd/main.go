package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli"
	"pgredis"
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
				cli.StringFlag{
					Name:     "database",
					Usage:    "the database connection details (eg. postgres://user:pass@host/dbname?sslmode=disable)",
					EnvVar:   "DATABASE_URL",
					Required: true,
				},
			},
			Action: func(ctx *cli.Context) error {
				server := pgredis.NewPgRedis(ctx.String("database"))
				return server.StartServer(ctx.String("bind"), ctx.String("port"))
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		logf("%+v", err)
		os.Exit(1)
	}
}
