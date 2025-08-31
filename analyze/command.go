package analyze

import (
	"github.com/urfave/cli/v2"
)

var Commands = &cli.Command{
	Name: "analyze",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "memory",
			Usage: "enables duckdb in-memory mode",
		},
	},
	Action: func(context *cli.Context) error {
		a, err := newAnalyzer(context.Bool("memory"))
		if err != nil {
			return err
		}
		return a.run()
	},
}
