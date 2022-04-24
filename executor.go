package go_web_archetype

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type Connection interface {
	sqlx.DB | sqlx.Tx
	Preparex(query string) (*sqlx.Stmt, error)
	Select(dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
}

func selectList[T Connection](conn T, sqlQuery string, args []interface{}, result interface{}) error {
	zap.L().Debug(fmt.Sprintf(`SQL: %s, args: %s`, sqlQuery, fmt.Sprint(args)))
	return conn.Select(result, sqlQuery, args...)
}

func get[T Connection](conn T, sqlQuery string, args []interface{}, result interface{}) error {
	zap.L().Debug(fmt.Sprintf(`SQL: %s, args: %s`, sqlQuery, fmt.Sprint(args)))
	return conn.Get(result, sqlQuery, args...)
}

func execute[T Connection](conn T, sqlQuery string, args []interface{}) (result sql.Result, err error) {
	var statement *sqlx.Stmt
	zap.L().Debug(fmt.Sprintf(`SQL: %s, args: %s`, sqlQuery, fmt.Sprint(args)))
	if statement, err = conn.Preparex(sqlQuery); err != nil {
		return
	}
	return statement.Exec(args...)
}

type queryExecutor[T Connection] struct {
	db   sqlx.DB
	conn T
}

func NewQueryExecutor(db sqlx.DB) queryExecutor[sqlx.DB] {
	return queryExecutor[sqlx.DB]{db: db, conn: db}
}
