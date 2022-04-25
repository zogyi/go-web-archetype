package go_web_archetype

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/***REMOVED***/go-web-archetype/log"
	"testing"
)

func TestExecutor_SelectList(t *testing.T) {
	log.InitLog(`/Users/zhongyi/workspace/golang/go-web-archetype/logs/`, `debug`)
	db, err := sqlx.Open(`mysql`, `***REMOVED***:***REMOVED***@tcp(***REMOVED***)/restaurant?charset=utf8&parseTime=true`)
	if err != nil {
		fmt.Println(`something wrong`)
		return
	}
	type resultType struct {
		Id     int    `db:"id"`
		Field1 string `db:"field1" json:"field1"`
	}
	queryHelper := NewDaoQueryHelper(true)
	queryHelper.Bind(resultType{}, `test`)
	executor := NewQueryExecutor(db, *queryHelper)
	err = executor.WithTxFunction(context.Background(), func(tx context.Context) (err error) {
		var result sql.Result
		//var queryResult = make([]resultType, 0)
		//result, err = executor.Insert(tx, resultType{Field1: `123124125125agsadgsadgsadg`}, ExtraQueryWrapper{})
		//if err != nil {
		//	return
		//}
		//result, err = executor.Insert(tx, resultType{Field1: `agas214agsgsgsdgsdfs`}, ExtraQueryWrapper{})
		//if err != nil {
		//	return
		//}
		//result, err = executor.Delete(tx, resultType{Field1: `i'm test 1, 2, 3, 4, 5, 6`}, ExtraQueryWrapper{})
		//if err != nil {
		//	return
		//}
		result, err = executor.Update(tx, resultType{Id: 18, Field1: `i'm test 1, 223232, 3, 4, 5, 6`}, ExtraQueryWrapper{})
		fmt.Println(result)
		return
	})
	//result := make([]resultType, 0)
	fmt.Println(err)
}
