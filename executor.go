package go_web_archetype

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
)

type Connection interface {
	*sqlx.DB | *sqlx.Tx
	Preparex(query string) (*sqlx.Stmt, error)
	Select(dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
}

func selectList[T Connection](conn T, sqlQuery string, args []interface{}, result interface{}) error {
	return conn.Select(result, sqlQuery, args...)
}

func get[T Connection](conn T, sqlQuery string, args []interface{}, result interface{}) error {
	return conn.Get(result, sqlQuery, args...)
}

func insertOrUpdate[T Connection](conn T, sqlQuery string, args []interface{}) (result sql.Result, err error) {
	var statement *sqlx.Stmt
	if statement, err = conn.Preparex(sqlQuery); err != nil {
		return
	}
	return statement.Exec(args...)
}
