package go_web_archetype

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/***REMOVED***/go-web-archetype/util"
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

type queryExecutor struct {
	db          *sqlx.DB
	queryHelper daoQueryHelper
}

func NewQueryExecutor(conn *sqlx.DB, helper daoQueryHelper) queryExecutor {
	return queryExecutor{db: conn, queryHelper: helper}
}

func (executor *queryExecutor) SelectPage(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (total uint64, err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.count(queryObj, queryWrapper); err == nil {
		if err = get(executor.db, sql, args, &total); err == nil {
			if sql, args, err = executor.queryHelper.selectPageQuery(queryObj, queryWrapper); err == nil {
				if tx, ok := util.ExtractTx(ctx); ok {
					selectList(tx, sql, args, resultSet)
				} else {
					err = selectList(executor.db, sql, args, resultSet)
				}
				return
			}
		}
	}
	return
}

func (executor *queryExecutor) SelectList(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.selectListQuery(queryObj, queryWrapper); err == nil {
		if tx, ok := util.ExtractTx(ctx); ok {
			return selectList(tx, sql, args, resultSet)
		}
		return selectList(executor.db, sql, args, resultSet)
	}
	return
}

func (executor *queryExecutor) Update(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.updateQuery(queryObj, wrapper); err == nil {
		if tx, ok := util.ExtractTx(ctx); ok {
			return executeQuery(tx, sql, args)
		}
		return executeQuery(executor.db, sql, args)
	}
	return
}

func (executor *queryExecutor) Delete(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.deleteQuery(queryObj, wrapper); err == nil {
		if tx, ok := util.ExtractTx(ctx); ok {
			return executeQuery(tx, sql, args)
		}
		return executeQuery(executor.db, sql, args)
	}
	return
}

func (executor *queryExecutor) Insert(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.insertQuery(queryObj, wrapper); err == nil {
		if tx, ok := util.ExtractTx(ctx); ok {
			return executeQuery(tx, sql, args)
		}
		return executeQuery(executor.db, sql, args)
	}
	return
}

func (executor *queryExecutor) WithTxFunction(ctx context.Context, txFunc func(context.Context) error) (err error) {
	var (
		tx *sqlx.Tx
	)
	tx, err = executor.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin transaction error: %w", err)
	}
	defer tx.Rollback()
	if err = txFunc(util.SetTxContext(ctx, tx)); err == nil {
		err = tx.Commit()
	}
	return
}
