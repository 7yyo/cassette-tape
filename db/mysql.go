package db

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type MySQL struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func NewMySQL(host string, port int, user string, password string, database string) *MySQL {
	return &MySQL{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Database: database,
	}
}

func (m *MySQL) Connect() (*sql.DB, error) {
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", m.User, m.Password, m.Host, m.Port, m.Database)
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	return db, nil
}
