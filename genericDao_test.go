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

type TestStruct1 struct {
	Id         null.Int        `db:"id" json:"id" archType:"primaryKey,autoFill"`
	Field1     null.String     `db:"field1" json:"field1"`
	Field2     null.String     `db:"field2" json:"field2"`
	Field3     null.String     `db:"field3" json:"field3"`
	Field4     util.MyNullTime `db:"field4" json:"field4"`
	Filed5     null.Int        `db:"field5" json:"field5"`
	CreateBy   null.String     `db:"create_by" json:"createBy" archType:"autoFill"`
	UpdateBy   null.String     `db:"update_by" json:"updateBy" archType:"autoFill"`
	CreateTime util.MyNullTime `db:"create_time" json:"createTime" archType:"autoFill"`
	Del        null.Bool       `db:"del" json:"del"  archType:"autoFill"`
}

func initGenericDao() *GenericDao {
	log.InitLog(``, `debug`)
	db, err := sqlx.Open(`mysql`, `root:***REMOVED***@tcp(localhost:3306)/test?charset=utf8&parseTime=true`)
	if err != nil {
		fmt.Println(err)
		panic(`can't open the database connection`)
	}

	dao := NewGenericDao(db)
	dao.Bind(TestStruct1{}, `test`)
	return dao
}

//func TestGenericDao_Delete(t *testing.T) {
//	fmt.Println(reflect.ValueOf(test).IsZero())
//	fmt.Println(reflect.ValueOf(test).IsValid())
//}

func TestGenericDao_TransferToSelectBuilder(t *testing.T) {
	//queryWrapper.Query.And = append(queryWrapper.Query.And, QueryItem{Field: "field4", Value: `1`, Operator: `eq`})

	//fmt.Println(reflect.TypeOf(result).Kind())
	//fmt.Println(result)
	//fmt.Println(err)
}

func TestGenericDao_Insert(t *testing.T) {
	dao := initGenericDao()
	queryWrapper := NewDefaultExtraQueryWrapper()
	setTime, err := time.Parse(`2006-01-02 15:04:05 -0700 MST`, `2021-09-02 15:04:05 +0800 UTC`)
	if err != nil {
		panic(err)
	}
	item1 := TestStruct1{
		Field1: null.StringFrom(`field1-1`),
		Field2: null.StringFrom(`field2-1`),
		Field3: null.StringFrom(`field3-1`),
		Field4: util.MyNullTime{Time: null.TimeFrom(setTime)},
		Filed5: null.IntFrom(1),
	}
	//item2 := TestStruct1{
	//	Field1: null.StringFrom(`field1-1`),
	//	Field2: null.StringFrom(`field2-1`),
	//	Field3: null.StringFrom(`field3-1`),
	//	Field4: null.StringFrom(`field4-1`),
	//	Filed5: null.IntFrom(1),
	//}
	//item3 := TestStruct1{
	//	Field1: null.StringFrom(`field1-1`),
	//	Field2: null.StringFrom(`field2-1`),
	//	Field3: null.StringFrom(`field3-1`),
	//	Field4: null.StringFrom(`field4-1`),
	//	Filed5: null.IntFrom(1),
	//}
	//item4 := TestStruct1{
	//	Field1: null.StringFrom(`field1-1`),
	//	Field2: null.StringFrom(`field2-1`),
	//	Field3: null.StringFrom(`field3-1`),
	//	Field4: null.StringFrom(`field4-1`),
	//	Filed5: null.IntFrom(1),
	//}
	if result, err := dao.InsertWithExtraQuery(item1, queryWrapper); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(result)
	}

}

func TestGenericDao_SelectWithExtraQuery(t *testing.T) {
	dao := initGenericDao()
	queryWrapper := NewDefaultExtraQueryWrapper()
	item := TestStruct1{
		//Id: null.IntFrom(1),
		//Field1: null.StringFrom(`升级一下是试试看 `),
		//CreateTime: util.MyNullTime{Time: null.TimeFrom(time.Now())},
	}
	//queryWrapper.Query.And = append(queryWrapper.Query.And, QueryItem{Field: "id", Operator: `in`, Value: []int{1, 2, 3, 4}})
	//queryWrapper.Query.And = append(queryWrapper.Query.And, QueryItem{Field: "field1", Operator: `like`, Value: `我是谁`})
	//queryWrapper.Query.And = append(queryWrapper.Query.And, QueryItem{Field: "field2", Operator: `like`, Value: `我是谁`})
	////queryWrapper.Query.And = append(queryWrapper.Query.And, QueryItem{Field: "field1", Operator: `like1`, Value: `我是谁`})
	//queryWrapper.Query.Or = append(queryWrapper.Query.Or, QueryItem{Field: "field1", Operator: `like`, Value: `我`})
	//queryWrapper.Query.Or = append(queryWrapper.Query.Or, QueryItem{Field: "field2", Operator: `like`, Value: `我`})
	result, err := dao.SelectWithExtraQuery(item, queryWrapper)
	fmt.Println(result)
	//fmt.Println(result.RowsAffected())
	fmt.Println(err)
}

func TestGenericDao_Delete(t *testing.T) {
	//dao := initGenericDao()
	//queryWrapper := NewDefaultExtraQueryWrapper()
	//item := TestStruct1{
	//	//CreateTime: util.MyNullTime{Time: null.TimeFrom(time.Now())},
	//}
	//dao.Delete()
}

func TestGenericDao_Validate(t *testing.T) {
	dao := initGenericDao()
	//queryWrapper := NewDefaultExtraQueryWrapper()
	item := TestStruct1{
		Id:         null.IntFrom(123),
		Field2:     null.StringFrom(`测试删除`),
		CreateTime: util.MyNullTime{Time: null.TimeFrom(time.Now())},
		Filed5:     null.IntFrom(0),
		Del:        null.BoolFrom(false)}
	eqCluase, setMap, _ := dao.Validate(item, Delete, `david`)
	fmt.Println(eqCluase)
	fmt.Println(setMap)
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
