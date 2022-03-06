package go_web_archetype

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type daoExecutor struct {
	DB 		  *sqlx.DB
	Tx 		  *sqlx.Tx
}

func (executor *daoExecutor) isTx() bool {
	if executor.Tx != nil {
		return true
	}
	return false
}

func (executor *daoExecutor) insertOrUpdate(sqlQuery string, args []interface{}) (result sql.Result, err error) {
	var (
		statement *sqlx.Stmt
	)
	zap.L().Sugar()
	if executor.isTx() {
		statement, err = executor.Tx.Preparex(sqlQuery)
	} else {
		statement, err = executor.DB.Preparex(sqlQuery)
	}
	zap.L().Sugar().Debugf("SQL: %s, Arguments: %s, Result %T", sqlQuery, args, result)
	if err != nil {
		return nil, err
	}
	return statement.Exec(args...)
}


func (executor *daoExecutor) selectList(sqlQuery string, args []interface{}, result interface{}) error {
	//resultSlice := reflect.MakeSlice(reflect.SliceOf(resultType), 0, 10)
	//x := reflect.New(resultSlice.Type())
	//x.Elem().Set(resultSlice)
	var err error
	zap.L().Sugar().Debugf("SQL: %s, Arguments: %s", sqlQuery, args)
	if executor.isTx() {
		executor.Tx = executor.Tx.Unsafe()
		err = executor.Tx.Select(result, sqlQuery, args...)
	} else {
		db := executor.DB.Unsafe()
		err = db.Select(result, sqlQuery, args...)
	}
	return err
}

func (executor *daoExecutor) get(sqlQuery string, args []interface{}, result interface{}) error {
	zap.L().Sugar().Debugf("SQL: %s, Arguments: %s", sqlQuery, args)
	var err error
	if executor.isTx() {
		err = executor.Tx.Get(result, sqlQuery, args...)
	} else {
		err = executor.DB.Get(result, sqlQuery, args...)
	}
	return err
}