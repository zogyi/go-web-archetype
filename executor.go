package go_web_archetype

import (
	"context"
	"database/sql"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/zogyi/go-web-archetype/util"
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

	SelectByQuery(ctx context.Context, resultSet any, query string, args ...interface{}) (err error)
	GetByQuery(ctx context.Context, resultSet any, query string, args ...interface{}) (err error)
	ExecuteByQuery(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error)

	Get(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (exist bool, err error)
	MustGet(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (err error)
	SelectPage(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (total uint64, err error)
	Select(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (err error)
	Update(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error)
	Delete(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error)
	Insert(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error)
	WithTxFunction(ctx context.Context, txFunc func(context.Context) error) (err error)

	GetTable(queryObj any) (string, bool)
	TransferToSelectBuilder(queryObj any, wrapper ExtraQueryWrapper, columns ...string) sq.SelectBuilder
	GetColumns(entity string) ([]string, bool)
}

func NewQueryExecutor(conn *sqlx.DB, helper DaoQueryHelper) (executor QueryExecutor) {
	executor = &QueryExecutorImpl{db: conn, queryHelper: helper}
	return
}

func (executor *QueryExecutorImpl) DB() *sqlx.DB {
	return executor.db
}

//GetTable get the table name
func (executor *QueryExecutorImpl) GetTable(queryObj any) (table string, exist bool) {
	return executor.queryHelper.GetEntityTable(queryObj)
}

//TransferToSelectBuilder create select builder according to the query object and the query wrapper
func (excutor *QueryExecutorImpl) TransferToSelectBuilder(queryObj any, wrapper ExtraQueryWrapper, columns ...string) sq.SelectBuilder {
	return excutor.queryHelper.TransferToSelectBuilder(queryObj, wrapper, columns...)
}

//GetColumns get all columns for an object
func (excutor *QueryExecutorImpl) GetColumns(entity string) (columns []string, exist bool) {
	return excutor.queryHelper.GetColumns(entity)
}

//SelectByQuery select query, using normal connection if the context doesn't have the transaction connection.
func (executor *QueryExecutorImpl) SelectByQuery(ctx context.Context, resultSet any, query string, args ...interface{}) (err error) {
	if tx, ok := util.ExtractTx(ctx); ok {
		return selectList(tx, query, args, resultSet)
	}
	return selectList(executor.db, query, args, resultSet)
}

//GetByQuery get query, using normal connection if the context doesn't have the transaction connection.
func (executor *QueryExecutorImpl) GetByQuery(ctx context.Context, resultSet any, query string, args ...interface{}) (err error) {
	if tx, ok := util.ExtractTx(ctx); ok {
		return get(tx, query, args, resultSet)
	}
	return get(executor.db, query, args, resultSet)
}

//ExecuteByQuery execute a query and return execute result and error, using normal connection if the context doesn't have the transaction connection.
func (executor *QueryExecutorImpl) ExecuteByQuery(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	if tx, ok := util.ExtractTx(ctx); ok {
		return executeQuery(tx, query, args)
	}
	return executeQuery(executor.db, query, args)
}

//Get support single tables query, generate the query according to the query object and the query wrapper
// using the query object to get the table name, add the column and value to the equal conditions of query criteria if the fields has db annotation and valid value
func (executor *QueryExecutorImpl) Get(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (exist bool, err error) {
	var (
		query string
		args  []interface{}
	)
	if query, args, err = executor.queryHelper.selectQuery(queryObj, queryWrapper); err != nil {
		return
	}

	if err = executor.GetByQuery(ctx, resultSet, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return exist, nil
		}
		return exist, err
	}
	return true, nil
}

//MustGet support single tables query, generate the query according to the query object and the query wrapper
// using the query object to get the table name, add the column and value to the equal conditions of query criteria if the fields has db annotation and valid value
func (executor *QueryExecutorImpl) MustGet(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.selectQuery(queryObj, queryWrapper); err != nil {
		return
	}
	return executor.GetByQuery(ctx, resultSet, sql, args...)
}

//SelectPage support single tables query, generate the query according to the query object and the query wrapper, get the total count along with the query result
// using the query object to get the table name, add the column and value to the equal conditions of query criteria if the fields has db annotation and valid value
func (executor *QueryExecutorImpl) SelectPage(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (total uint64, err error) {
	var (
		sql  string
		args []interface{}
	)
	if queryWrapper.Pagination.PageSize <= 0 {
		queryWrapper.Pagination.PageSize = 10
	}
	if sql, args, err = executor.queryHelper.count(queryObj, queryWrapper); err != nil {
		return
	}
	if err = executor.GetByQuery(ctx, &total, sql, args...); err != nil {
		return
	}
	if sql, args, err = executor.queryHelper.selectPageQuery(queryObj, queryWrapper); err != nil {
		return
	}
	return total, executor.SelectByQuery(ctx, resultSet, sql, args...)
}

//Select support single tables query, generate the query according to the query object and the query wrapper
// using the query object to get the table name, add the column and value to the equal conditions of query criteria if the fields has db annotation and valid value
func (executor *QueryExecutorImpl) Select(ctx context.Context, queryObj any, queryWrapper ExtraQueryWrapper, resultSet any) (err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.selectQuery(queryObj, queryWrapper); err != nil {
		return
	}
	return executor.SelectByQuery(ctx, resultSet, sql, args...)
}

//Update generate the update query, the set map comes from the query object and the where condition is generate by the query wrapper
func (executor *QueryExecutorImpl) Update(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.updateQuery(queryObj, wrapper); err != nil {
		return
	}
	return executor.ExecuteByQuery(ctx, sql, args...)
}

//Delete generate delete query, only support one table's query
// using the query object to get the table name, add the column and value to the equal conditions of query criteria if the fields has db annotation and valid value
func (executor *QueryExecutorImpl) Delete(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.deleteQuery(queryObj, wrapper); err != nil {
		return
	}
	return executor.ExecuteByQuery(ctx, sql, args...)
}

//Insert generate insert query, only support one table's query
// using the query object to get the table name, add the column and value to the set map for the insert query if the fields has db annotation and valid value
func (executor *QueryExecutorImpl) Insert(ctx context.Context, queryObj any, wrapper ExtraQueryWrapper) (result sql.Result, err error) {
	var (
		sql  string
		args []interface{}
	)
	if sql, args, err = executor.queryHelper.insertQuery(queryObj, wrapper); err != nil {
		return
	}
	return executor.ExecuteByQuery(ctx, sql, args...)
}

//WithTxFunction a unified method to execute the queries in one transaction, auto begin and commit, rollback if any point has error
// txFunc is a method to execute the transactional queries, inject with a transaction connection in the context
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
