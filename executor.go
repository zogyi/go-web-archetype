package go_web_archetype

import (
	"context"
	"database/sql"
	"fmt"
	sq "github.com/Masterminds/squirrel"
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

type QueryExecutorImpl struct {
	db          *sqlx.DB
	queryHelper DaoQueryHelper
}

type QueryExecutor interface {
	DB() *sqlx.DB
	SelectPage(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (total uint64, err error)
	Select(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (err error)
	Update(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error)
	Delete(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error)
	Insert(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error)
	WithTxFunction(ctx context.Context, txFunc func(context.Context) error) (err error)

	GetTable(queryObj any) (string, bool)
	TransferToSelectBuilder(queryObj any, wrapper ExtraQueryWrapper) sq.SelectBuilder
}

func NewQueryExecutor(conn *sqlx.DB, helper DaoQueryHelper) (executor QueryExecutor) {
	executor = &QueryExecutorImpl{db: conn, queryHelper: helper}
	return
}

func (executor *QueryExecutorImpl) DB() *sqlx.DB {
	return executor.db
}

func (executor *QueryExecutorImpl) GetTable(queryObj any) (table string, exist bool) {
	return executor.queryHelper.GetEntityTable(queryObj)
}

func (excutor *QueryExecutorImpl) TransferToSelectBuilder(queryObj any, wrapper ExtraQueryWrapper) sq.SelectBuilder {
	return excutor.queryHelper.TransferToSelectBuilder(queryObj, wrapper)
}

func (executor *QueryExecutorImpl) SelectPage(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (total uint64, err error) {
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

func (executor *QueryExecutorImpl) Select(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (err error) {
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

func (executor *QueryExecutorImpl) Update(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
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

func (executor *QueryExecutorImpl) Delete(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
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

func (executor *QueryExecutorImpl) Insert(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
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

func (executor *QueryExecutorImpl) WithTxFunction(ctx context.Context, txFunc func(context.Context) error) (err error) {
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
