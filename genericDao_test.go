package go_web_archetype

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/***REMOVED***/go-web-archetype/log"
	"github.com/***REMOVED***/go-web-archetype/util"
	"gopkg.in/guregu/null.v3"
	"reflect"
	"strconv"
	"testing"
)

func initGenericDao() *GenericDao {
	log.InitLog(``, `debug`)
	db, err := sqlx.Open(`mysql`, `root:***REMOVED***@tcp(localhost:3306)/test?charset=utf8`)
	if err != nil {
		fmt.Println(err)
		panic(`can't open the database connection`)
	}
	return NewGenericDao(db)
}

func TestGenericDao_TransferToSelectBuilder(t *testing.T) {
	type TestStruct1 struct {
		Id     null.Int    `db:"id" json:"id"`
		Field1 null.String `db:"field1" json:"field1"`
		Field2 null.String `db:"field2" json:"field2"`
		Field3 null.String `db:"field3" json:"field3"`
		Field4 null.String `db:"field4" json:"field4"`
		CreateTime util.MyNullTime `db:"create_time" json:"createTime" archType:"autoFill"`
	}
	dao := initGenericDao()
	dao.Bind(TestStruct1{}, `test`)
	queryWrapper := NewDefaultExtraQueryWrapper()
	queryWrapper.Query.And = append(queryWrapper.Query.And, QueryItem{Field: "field4", Value: `1 or 2 = 2`, Operator: `eq`})
	item := TestStruct1{Field1: null.StringFrom(`我是谁`)}
	result, err := dao.InsertWithExtraQuery(item, queryWrapper)
	fmt.Println(reflect.TypeOf(result).Kind())
	fmt.Println(result)
	fmt.Println(err)
}

func BenchmarkGenericDao_TransferToSelectBuilder(b *testing.B) {
	type TestStruct1 struct {
		Id     null.Int    `db:"id" json:"id"`
		Field1 null.String `db:"field1" json:"field1"`
		Field2 null.String `db:"field2" json:"field2"`
		Field3 null.String `db:"field3" json:"field3"`
		Field4 null.String `db:"field4" json:"field4"`
	}
	dao := initGenericDao()
	dao.Bind(TestStruct1{}, `test`)
	queryWrapper := NewDefaultExtraQueryWrapper()
	queryWrapper.Query.And = append(queryWrapper.Query.And, QueryItem{Field: "field4", Value: `1 or 2 = 2`, Operator: `eq`})
	for n := 0; n < b.N; n++ {
		item := TestStruct1{Field1: null.StringFrom(`我是谁` + strconv.Itoa(n))}
		dao.InsertWithExtraQuery(item, queryWrapper)
	}
}
