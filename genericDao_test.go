package go_web_archetype

import (
	"fmt"
	"gopkg.in/guregu/null.v3"
	"testing"
)

func TestGenericDao_TransferToSelectBuilder(t *testing.T) {
	type TestStruct1 struct {
		Field1 null.String `db:"field1"`
		Field2 null.String `db:"field2"`
		Field3 null.String `db:"field3"`
		Field4 null.String `db:"field4"`
	}
	dao := GenericDao{}
	//dao.Bind(TestStruct1{},`test`)
	queryWrapper := NewDefaultExtraQueryWrapper()
	builder, args := dao.TransferToSelectBuilder(TestStruct1{}, queryWrapper)
	querySQL, _, err := builder.ToSql()
	fmt.Println(querySQL)
	fmt.Println(args)
	if err != nil {
		t.Error(err.Error())
	}
}