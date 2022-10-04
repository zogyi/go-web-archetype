package go_web_archetype

import (
	"encoding/json"
	"fmt"
	"github.com/zogyi/go-web-archetype/base"
	"github.com/zogyi/go-web-archetype/log"
	"gopkg.in/guregu/null.v3"
	"testing"
	"time"
)

type TestStruct1 struct {
	Field1 null.String `db:"field1" json:"field1"`
	Field2 null.Time   `db:"field2" json:"field2"`
	Field3 null.Bool   `db:"field3" json:"field3"`
	base.CommonFields
	base.CommonDel
}

type TestStruct2 struct {
	Id     null.Int    `db:"id" json:"id" archType:"primaryKey,autoFill"`
	Field1 null.String `db:"field1" json:"field1"`
	base.CommonFields
}

func initGenericDao() *DaoQueryHelper {
	log.InitLog(``, `debug`)
	//db, err := sqlx.Open(`mysql`, `***REMOVED***:***REMOVED***@tcp(***REMOVED***)/restaurant?charset=utf8&parseTime=true`)
	//if err != nil {
	//	fmt.Println(err)
	//	panic(`can't open the database connection`)
	//}

	//dao := NewGenericDao(db)
	//dao.Bind(TestStruct1{}, `test`)
	return nil
}

//func TestGenericDao_Delete(t *testing.T) {
//	fmt.Println(reflect.ValueOf(test).IsZero())
//	fmt.Println(reflect.ValueOf(test).IsValid())
//}

func TestGenericDao_TransferToSelectBuilder(t *testing.T) {
	//queryWrapper.QueryExtension.And = append(queryWrapper.QueryExtension.And, QueryItem{Field: "field4", Value: `1`, Operator: `eq`})

	//fmt.Println(reflect.TypeOf(result).Kind())
	//fmt.Println(result)
	//fmt.Println(err)
}

func TestGenericDao_deleteQuery(t *testing.T) {
	dao := DaoQueryHelper{}
	dao.setFullTableExecute(false)
	dao.AddCustomType(base.CommonFields{}, base.CommonDel{})
	dao.Bind(TestStruct1{}, `test`)

	item1 := TestStruct1{
		Field1: null.StringFrom(`field2-1`),
		Field2: null.TimeFrom(time.Now()),
		Field3: null.BoolFrom(false),
		//Field4: util.MyNullTime{Time: null.TimeFrom(setTime)},
		//Filed5: null.IntFrom(1),
	}
	sql, args, err := dao.deleteQuery(item1, base.ExtraQueryWrapper{})
	fmt.Println(sql)
	fmt.Println(args)
	fmt.Println(err)
}

func TestDaoQueryHelper_insertQuery(t *testing.T) {
	dao := DaoQueryHelper{}
	dao.setFullTableExecute(true)
	dao.AddCustomType(base.CommonFields{}, base.CommonDel{})
	dao.Bind(TestStruct1{}, `test`)
	item1 := TestStruct1{
		Field1:       null.StringFrom(`field2-1`),
		Field2:       null.TimeFrom(time.Now()),
		Field3:       null.BoolFrom(false),
		CommonFields: base.CommonFields{Id: null.IntFrom(1)},
	}
	fmt.Println(dao.insertQuery(item1, base.ExtraQueryWrapper{}))

	item1 = TestStruct1{
		Field1: null.StringFrom(`field2-1`),
	}
	fmt.Println(dao.insertQuery(item1, base.ExtraQueryWrapper{}))
}

func TestDaoQueryHelper_updateQuery(t *testing.T) {
	dao := DaoQueryHelper{}
	dao.AddCustomType(base.CommonFields{}, base.CommonDel{})
	dao.Bind(TestStruct1{}, `test`)
	item1 := TestStruct1{
		Field1:       null.StringFrom(`field2-1`),
		CommonFields: base.CommonFields{Id: null.IntFrom(1)},
	}
	fmt.Println(dao.updateQuery(item1, base.ExtraQueryWrapper{}))

	item1 = TestStruct1{}
	fmt.Println(dao.updateQuery(item1, base.ExtraQueryWrapper{}))
}

func TestDaoQueryHelper_selectQuery(t *testing.T) {
	dao := DaoQueryHelper{}
	dao.AddCustomType(base.CommonFields{}, base.CommonDel{})
	dao.Bind(TestStruct1{}, `test`)

	item1 := TestStruct1{
		Field1:       null.StringFrom(`field2-1`),
		CommonFields: base.CommonFields{Id: null.IntFrom(1)},
	}
	fmt.Println(dao.selectQuery(item1, base.ExtraQueryWrapper{}))

	//item1 = TestStruct1{}
	//fmt.Println(dao.updateQuery(item1, ExtraQueryWrapper{}))
}

func getQuery() (query base.Query, err error) {
	jsonString := `	{
	  "connector": "OR",
	  "conditions": [
	    {
	      "field": "field1",
	      "value": "5",
	      "operator": "gt"
	    },
	    {
	      "connector": "OR",
	      "conditions": [
	        {
	          "field": "field2",
	          "value": "%b%",
	          "operator": "eq"
	        }
	      ]
	    }
	  ]
	}`
	err = json.Unmarshal([]byte(jsonString), &query)
	return query, nil
}

func TestGenericDao_SelectWithExtraQuery(t *testing.T) {
	//dao := initGenericDao()
	//queryWrapper := NewDefaultExtraQueryWrapper()
	//item := TestStruct1{
	//	Id: null.IntFrom(1),
	//	//Field1: null.StringFrom(`升级一下是试试看 `),
	//	//CreateTime: util.MyNullTime{Time: null.TimeFrom(time.Now())},
	//}
	//query, err := getQuery()
	//fmt.Println(err)
	//fmt.Println(query)
	//queryWrapper.QueryExtension = &QueryExtension{Query: query}
	////queryWrapper.QueryExtension.And = append(queryWrapper.QueryExtension.And, QueryItem{Field: "field1", Operator: `like`, Value: `我是谁`})
	////queryWrapper.QueryExtension.And = append(queryWrapper.QueryExtension.And, QueryItem{Field: "field2", Operator: `like`, Value: `我是谁`})
	//////queryWrapper.QueryExtension.And = append(queryWrapper.QueryExtension.And, QueryItem{Field: "field1", Operator: `like1`, Value: `我是谁`})
	////queryWrapper.QueryExtension.Or = append(queryWrapper.QueryExtension.Or, QueryItem{Field: "field1", Operator: `like`, Value: `我`})
	////queryWrapper.QueryExtension.Or = append(queryWrapper.QueryExtension.Or, QueryItem{Field: "field2", Operator: `like`, Value: `我`})
	//result := make([]TestStruct1, 0)
	//fmt.Println(result)
	////fmt.Println(result.RowsAffected())
	//fmt.Println(err)
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
		//Field2:     null.StringFrom(`测试删除`),
		//CreateTime: util.MyNullTime{Time: null.TimeFrom(time.Now())},
		//Filed5:     null.IntFrom(0),
		//Del:        null.BoolFrom(false)
	}
	eqCluase := dao.validate(item)
	fmt.Println(eqCluase)
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
	//queryWrapper := NewDefaultExtraQueryWrapper()
	//query, err := getQuery()
	//if err != nil {
	//	panic(err)
	//}
	//queryWrapper.QueryExtension = &QueryExtension{Query: query}
	//for n := 0; n < b.N; n++ {
	//	item := TestStruct1{Field1: null.StringFrom(`我是谁` + strconv.Itoa(n))}
	//	dao.InsertWithExtraQuery(item, queryWrapper)
	//}
}
