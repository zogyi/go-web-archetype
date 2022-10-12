package go_web_archetype

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/zogyi/go-web-archetype/log"
	"gopkg.in/guregu/null.v3"
	"testing"
	"time"
)

type resultType struct {
	Id     int         `db:"id"`
	Field1 null.String `db:"field1" json:"field1"`
	Field2 null.String `db:"field2" json:"field2"`
	Field3 null.String `db:"field3" json:"field3"`
	Field4 null.String `db:"field4" json:"field4"`
}

func prepareExecutor() QueryExecutor {
	log.InitLog(`/Users/zhongyi/workspace/golang/go-web-archetype/logs/`, `debug`)
	queryHelper := NewDaoQueryHelper()
	//queryHelper.setFullTableExecute(true)
	queryHelper.Bind(resultType{}, `test`)
	return NewQueryExecutorForTesting(*queryHelper)
}

func TestExecutor_SelectList(t *testing.T) {
	ast := assert.New(t)
	executor := prepareExecutor()
	result := make([]resultType, 0)
	err := executor.Select(context.Background(), resultType{}, &result)
	ast.Nil(err, `execute the select failed`)
}

func testPtrToStruct(ptrObj any) {

}

func TestQueryExecutorImpl_SelectPage(t *testing.T) {
	ast := assert.New(t)
	executor := prepareExecutor()
	result := make([]resultType, 0)
	total, err := executor.SelectPage(context.Background(), resultType{}, &result)
	ast.Nil(err, `execute the select failed`)
	ast.GreaterOrEqual(total, uint64(0), `the count is not greater or equal 0`)
}

func TestQueryExecutorImpl_Get(t *testing.T) {
	ast := assert.New(t)
	executor := prepareExecutor()
	result := resultType{}
	exist, err := executor.Get(context.Background(), resultType{}, &result)
	ast.Nil(err, `execute the select failed`)
	ast.Truef(exist, `no result found`)
}

func TestQueryExecutorImpl_GetById(t *testing.T) {
	executor := prepareExecutor()
	result := resultType{}
	executor.GetById(context.Background(), null.IntFrom(1), &result)

}

func TestQueryExecutorImpl_Insert(t *testing.T) {
	ast := assert.New(t)
	executor := prepareExecutor()
	now := time.Now().String()
	result := resultType{Field1: null.StringFrom(`this is the field1, time: ` + now), Field2: null.StringFrom(`this is the field2, time: ` + now)}
	queryResult, err := executor.Insert(context.Background(), result)
	ast.Nil(err, `execute the select failed`)
	effected, _ := queryResult.RowsAffected()
	ast.GreaterOrEqual(effected, int64(0), `effected row is not greater or equal 0`)
}

func TestQueryExecutorImpl_Delete(t *testing.T) {
	ast := assert.New(t)
	executor := prepareExecutor()
	result := resultType{}
	queryResult, err := executor.Delete(context.Background(), result)
	ast.Nil(err, `execute the select failed`)
	effected, _ := queryResult.RowsAffected()
	ast.GreaterOrEqual(effected, int64(0), `effected row is not greater or equal 0`)
}
