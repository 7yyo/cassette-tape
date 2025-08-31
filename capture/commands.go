package capture

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

const (
	device        = "device"
	port          = "port"
	defaultDevice = "lo0"
	defaultPort   = 3306
	level         = "level"
	defaultLevel  = "info"
)

var Commands = &cli.Command{
	Name: "capture",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name: device, Value: defaultDevice,
		},
		&cli.IntFlag{
			Name: port, Value: defaultPort,
		},
		&cli.StringFlag{
			Name: level, Usage: "info and debug",
			Value: defaultLevel,
		},
	},
	Action: func(context *cli.Context) error {
		c, err := newCapture(
			context.String(device),
			context.Int(port),
			context.String(level),
		)
		if err != nil {
			return fmt.Errorf("create capture failed: %w", err)
		}
		return c.run()
	},
}
