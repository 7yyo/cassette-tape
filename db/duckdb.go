package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const (
	TableName = "queries"
	ddl       = `CREATE TABLE %s AS
		SELECT * FROM read_json('%s', auto_detect = false,
		COLUMNS = {
			'timestamp': 'TIMESTAMP_MS', 
			'conn': 'INT', 
			'type': 'VARCHAR(11)', 
			'digest': 'VARCHAR(64)', 
			'text': 'TEXT'})`
)

var dbName string

type DuckDB struct {
	Conn *sql.DB
}

func NewDuckDB(option string, mm bool) (*DuckDB, error) {
	if mm {
		dbName = ":memory:"
	} else {
		dbName = "duckdb.db"
	}

	conn, err := sql.Open("duckdb", dbName)
	if err != nil {
		return nil, fmt.Errorf("open db failed: %w", err)
	}

	_, err = conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", TableName))
	if err != nil {
		return nil, fmt.Errorf("drop db failed: %w", err)
	}

	_, err = conn.Exec(fmt.Sprintf(ddl, TableName, option))
	if err != nil {
		return nil, fmt.Errorf("create db failed: %w", err)
	}

	arch := fmt.Sprintf("%s_%s", runtime.GOOS, runtime.GOARCH)

	extensionPath, err := getBinaryPath("db", "duckdb", "json_extension", arch, "json.duckdb_extension")
	if err != nil {
		return nil, fmt.Errorf("get extension path failed: %w", err)
	}

	_, err = conn.Exec(fmt.Sprintf("LOAD '%s'", extensionPath))
	if err != nil {
		return nil, fmt.Errorf("load extension failed: path: %s, %w", extensionPath, err)
	}

	return &DuckDB{
		Conn: conn}, nil
}

func (d *DuckDB) Close() error {
	return d.Conn.Close()
}

func getBinaryPath(args ...string) (string, error) {
	rootPath, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("get binary path failed: %w", err)
	}
	dir := filepath.Dir(rootPath)
	path := append([]string{dir}, args...)
	return filepath.Join(path...), nil
}
