package go_web_archetype

import (
	"github.com/jmoiron/sqlx"
)

type Connection interface {
	*sqlx.DB | *sqlx.Tx
	Preparex(query string) (*sqlx.Stmt, error)
	Select(dest interface{}, query string, args ...interface{}) error
	//Get(dest interface{}, query string, args ...interface{}) error
}

func selectList[T Connection](conn T, sqlQuery string, args []interface{}, result interface{}) error {
	return conn.Select(result, sqlQuery, args...)
}
