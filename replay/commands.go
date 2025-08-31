package replay

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

const (
	host     = "host"
	port     = "port"
	user     = "user"
	password = "password"
	database = "db"
	readonly = "readonly"
	memory   = "memory"
)

var Commands = &cli.Command{
	Name: "replay",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name: host, Value: "127.0.0.1",
		},
		&cli.IntFlag{
			Name: port, Value: 3306,
		},
		&cli.StringFlag{
			Name: user, Value: "root",
		},
		&cli.StringFlag{
			Name: password, Value: "",
		},
		&cli.StringFlag{
			Name: database, Value: "test",
		},
		&cli.BoolFlag{
			Name: readonly, Value: true, Usage: "only replay select statements",
		},
		&cli.BoolFlag{
			Name: memory, Value: false, Usage: "enables duckdb in-memory mode",
		},
	},
	Action: func(context *cli.Context) error {
		replayer, err := newReplayer(
			context.String(host),
			context.Int(port),
			context.String(user),
			context.String(password),
			context.String(database),
			context.Bool(readonly),
			context.Bool(memory),
		)
		if err != nil {
			return fmt.Errorf("create replayer failed: %w", err)
		}
		return replayer.run()
	},
}
