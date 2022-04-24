package go_web_archetype

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type Connection interface {
	*sqlx.DB | *sqlx.Tx
	Preparex(query string) (*sqlx.Stmt, error)
	Select(dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
}

func selectList[T Connection](conn T, sqlQuery string, args []interface{}, result any) error {
	zap.L().Debug(fmt.Sprintf(`SQL: %s, args: %s`, sqlQuery, fmt.Sprint(args)))
	return conn.Select(result, sqlQuery, args...)
}

func get[T Connection](conn T, sqlQuery string, args []interface{}, result interface{}) error {
	zap.L().Debug(fmt.Sprintf(`SQL: %s, args: %s`, sqlQuery, fmt.Sprint(args)))
	return conn.Get(result, sqlQuery, args...)
}

func executeQuery[T Connection](conn T, sqlQuery string, args []interface{}) (result sql.Result, err error) {
	var statement *sqlx.Stmt
	zap.L().Debug(fmt.Sprintf(`SQL: %s, args: %s`, sqlQuery, fmt.Sprint(args)))
	if statement, err = conn.Preparex(sqlQuery); err != nil {
		return
	}
	return statement.Exec(args...)
}

type queryExecutor[T Connection] struct {
	db          T
	queryHelper DaoQueryHelper
}

func NewQueryExecutor[T Connection](conn T, helper DaoQueryHelper) queryExecutor[T] {
	return queryExecutor[T]{db: conn, queryHelper: helper}
}

func (executor *queryExecutor[T]) SelectPage(queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (total uint64, err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.count(queryObj, queryWrapper); err == nil {
		if err = get(executor.db, sql, args, &total); err == nil {
			if sql, args, err = executor.queryHelper.selectPageQuery(queryObj, queryWrapper); err == nil {
				err = selectList(executor.db, sql, args, resultSet)
				return
			}
		}
	}
	return
}

func (executor *queryExecutor[T]) SelectList(queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.selectListQuery(queryObj, queryWrapper); err == nil {
		return selectList(executor.db, sql, args, resultSet)
	}
	return
}

func (executor *queryExecutor[T]) Update(queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.updateQuery(queryObj, wrapper); err == nil {
		return executeQuery(executor.db, sql, args)
	}
	return
}

func (executor *queryExecutor[T]) Delete(queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.deleteQuery(queryObj, wrapper); err == nil {
		return executeQuery(executor.db, sql, args)
	}
	return
}
