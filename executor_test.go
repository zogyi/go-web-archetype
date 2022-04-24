package go_web_archetype

import (
	"testing"
)

func TestExecutor_SelectList(t *testing.T) {
	type resultType struct {
		Id     int    `db:"id"`
		Field1 string `db:"field1"`
	}
	//dao := initGenericDao()
	//tx, err := dao.DB().Beginx()
	//defer tx.Rollback()
	//db := dao.DB().Unsafe()
	//insertResult, err := execute(db, `insert into test(field1) values(?)`, []interface{}{`test`})
	//fmt.Println(insertResult)
	//fmt.Println(err)
	//tx.Commit()
}
