package analyze

import (
	"cassette-tape/db"
	o "cassette-tape/option"
)

type analyzer struct {
	duckdb *db.DuckDB
}

func newAnalyzer(mm bool) (*analyzer, error) {

	option, err := o.GetOption()
	if err != nil {
		return nil, err
	}
	duckdb, err := db.NewDuckDB(option, mm)
	if err != nil {
		return nil, err
	}
	return &analyzer{
		duckdb: duckdb,
	}, nil
}

func (a *analyzer) run() error {
	r := newReport(a.duckdb)
	r.render()
	return nil
}
