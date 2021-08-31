package go_web_archetype

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/***REMOVED***/go-web-archetype/log"
	"github.com/***REMOVED***/go-web-archetype/util"
	"gopkg.in/guregu/null.v3"
	"strconv"
	"testing"
	"time"
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
		Id     null.Int    `db:"id" json:"id" archType:"primaryKey"`
		Field1 null.String `db:"field1" json:"field1"`
		Field2 null.String `db:"field2" json:"field2"`
		Field3 null.String `db:"field3" json:"field3"`
		Field4 null.String `db:"field4" json:"field4"`
		Filed5 null.Int    `db:"field5" json:"field5"`
		CreateTime util.MyNullTime `db:"create_time" json:"createTime" archType:"autoFill"`
		Del    null.Bool 	`db:"del" json:"del"  archType:"autoFill"`
	}
	dao := initGenericDao()
	dao.Bind(TestStruct1{}, `test`)
	queryWrapper := NewDefaultExtraQueryWrapper()
	//queryWrapper.Query.And = append(queryWrapper.Query.And, QueryItem{Field: "field4", Value: `1`, Operator: `eq`})
	item := TestStruct1{Id: null.IntFrom(8506),
		Field2: null.StringFrom(`测试删除`),
		CreateTime: util.MyNullTime{Time: null.TimeFrom(time.Now())},
		Filed5: null.IntFrom(0),
		Del: null.BoolFrom(false)}
	err := dao.DeleteWithExtraQuery(item, queryWrapper)
	//fmt.Println(reflect.TypeOf(result).Kind())
	//fmt.Println(result)
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
