package go_web_archetype

import "testing"

func TestExecutor_SelectList(t *testing.T) {
	type resultType struct {
		Id     int    `db:"id"`
		Field1 string `db:"field1"`
	}
	result := make([]resultType, 0)
	dao := initGenericDao()
	tx, err := dao.DB().Beginx()
	defer tx.Rollback()
	err = selectList(dao.DB(), `select * from test`, []interface{}{}, &result)
	println(err)
	tx.Commit()
}
