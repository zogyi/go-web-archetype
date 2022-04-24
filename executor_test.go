package go_web_archetype

import (
	"context"
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
		Field1 string `db:"field1"`
	}
	queryHelper := DaoQueryHelper{}
	queryHelper.Bind(resultType{}, `test`)
	executor := NewQueryExecutor(db, queryHelper)
	//result := make([]resultType, 0)
	result, err := executor.Insert(context.Background(), resultType{Field1: `i'm test 1, 2, 3, 4, 5, 6`}, ExtraQueryWrapper{})
	fmt.Println(err)
	fmt.Println(result)
}
