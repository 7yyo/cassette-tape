package replay

import (
	"cassette-tape/db"
	o "cassette-tape/option"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type replayer struct {
	wm       *workloadManager
	duckdb   *db.DuckDB
	readonly bool
}

func newReplayer(host string, port int, user, password, database string, readonly, mm bool) (*replayer, error) {

	option, err := o.GetOption()
	if err != nil {
		return nil, fmt.Errorf("get option failed: %w", err)
	}

	startTime := time.Now()
	duckdb, err := db.NewDuckDB(option, mm)
	if err != nil {
		return nil, fmt.Errorf("new engine failed: %w", err)
	}
	log.Info("new engine completed", zap.Duration("duration", time.Since(startTime)))

	return &replayer{
		wm: newWorkloadManager(
			duckdb, db.NewMySQL(host, port, user, password, database), readonly),
		duckdb:   duckdb,
		readonly: readonly,
	}, nil
}

func (r *replayer) run() error {
	return r.wm.run()
}
