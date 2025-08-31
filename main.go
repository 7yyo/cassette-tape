package main

import (
	"cassette-tape/analyze"
	"cassette-tape/capture"
	"cassette-tape/replay"
	"os"

	"github.com/pingcap/log"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func main() {

	app := &cli.App{
		Name: "📼 cassette-tape",
		Commands: []*cli.Command{
			capture.Commands,
			analyze.Commands,
			replay.Commands,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error("error occurred", zap.Error(err))
	}
}
