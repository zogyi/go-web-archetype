package go_web_archetype

import (
	"database/sql"
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/***REMOVED***/go-web-archetype/util"
	"gopkg.in/guregu/null.v3"
	"reflect"
	"strings"
)

//TODO: 1. extract the field and columns mapping and save into a map
type QueryItem struct {
	Field 		string		`json:"field"`
	Operator    string		`json:"operator"`
	Value       string 		`json:"value"`
}

type QueryWrapper struct {
	And   []QueryItem 		`json:"and"`
	Group map[string]string	`json:"group"`
}

type ExtraQueryWrapper struct {
	CurrentUsername string
	Pagination      *util.Pagination
	Query           *QueryWrapper
	NullFields		[]string
}

const (
	FIXED_COLUMN_ID        string = `id`
	FIXED_COLUMN_CREATE_BY string = `create_by`
	FIXED_COLUMN_UPDATE_BY string = `update_by`
)

type fieldInfo struct {
	Field		string
	JSONField	string
	TableField	string
	Type 		string
}

type GenericDao struct {
	DB                 *sqlx.DB
	bondEntities       []interface{}
	entityTableMapping map[string]string
	entityFieldMapping map[string]map[string]*fieldInfo
	commonFields 	   util.CommonFields
}

func (gd *GenericDao) GetBondEntities() []interface{} {
	return gd.bondEntities
}

func (gd *GenericDao) GetEntityTableMapping() map[string]string {
	return gd.entityTableMapping
}

func (gd *GenericDao) Bind(interf interface{}, table string) {
	crtIrf := reflect.TypeOf(interf)
	if gd.entityTableMapping == nil {
		gd.entityTableMapping = make(map[string]string)
	}
	if gd.entityFieldMapping == nil {
		gd.entityFieldMapping = make(map[string]map[string]*fieldInfo)
	}
	gd.entityTableMapping[crtIrf.Name()] = table
	fieldsMapping := make(map[string]*fieldInfo)
	fieldCount := reflect.TypeOf(interf).NumField()
	for i := 0; i < fieldCount; i++ {
		currentField := reflect.TypeOf(interf).Field(i)
		if currentField.Type == reflect.TypeOf(gd.commonFields) {
			for k := 0; k < reflect.TypeOf(gd.commonFields).NumField(); k++ {
				commonField := reflect.TypeOf(gd.commonFields).Field(k)
				fieldsMapping[commonField.Name] = getFieldInfo(commonField)
			}
		} else {
			fieldsMapping[currentField.Name] = getFieldInfo(currentField)
		}
	}
	gd.entityFieldMapping[crtIrf.Name()] = fieldsMapping
	gd.bondEntities = append(gd.bondEntities, interf)
	fmt.Println(fieldsMapping)
}

func getFieldInfo(field reflect.StructField) *fieldInfo {
	dbTag := field.Tag.Get("db")
	var tableFiled, jsonField string
	if dbTag == `` {
		tableFiled = strings.ToLower(field.Name)
	} else if dbTag != `-` {
		tableFiled = dbTag
	}
	jsonTag := field.Tag.Get(`json`)
	if strings.TrimSpace(jsonTag) != `` || strings.TrimSpace(jsonTag) != `-` {
		jsonField = jsonTag
	}
	return &fieldInfo{JSONField: jsonField, TableField: tableFiled, Type: field.Type.Name()}
}

func (gd *GenericDao) GetById(intf interface{}, id uint64) (interface{}, error) {
	return gd.Get(intf, []string{`id`}, []interface{}{id}, false)
}

func (gd *GenericDao) Get(intf interface{}, columns []string, args []interface{}, forceResult bool) (interface{}, error) {
	if reflect.TypeOf(intf).Kind() != reflect.Struct {
		return nil, errors.New(`the interface should be a struct non of pointer`)
	}
	table := gd.entityTableMapping[reflect.TypeOf(intf).Name()]
	if table == `` || strings.TrimSpace(table) == `` {
		return nil, errors.New(`no mapping found for the interface` + reflect.TypeOf(intf).Name())
	}
	if len(columns) == 0 || len(columns) != len(args) {
		return nil, errors.New(`no query criteria found`)
	}
	columns = append(columns, `del`)
	args = append(args, 0)
	whereClause := strings.Join(columns, ` = ?  and `) + ` = ?`
	querySql, _, err := sq.Select(`*`).From(table).Where(whereClause).ToSql()
	if err != nil {
		panic(err)
	}
	entity := reflect.New(reflect.TypeOf(intf))
	err = gd.DB.Get(entity.Interface(), querySql, args...)
	if err != nil {
		panic(err)
	}
	return entity.Interface(), nil
}

func (gd *GenericDao) GetOne(intf interface{}) (interface{}, error)  {
	queryResult, err := gd.Select(intf)
	if err != nil {
		return nil, err
	}
	s := reflect.ValueOf(queryResult)
	if s.Kind() == reflect.Ptr {
		s = s.Elem()
	}
	if s.Kind() == reflect.Slice {
		if s.Len() == 1 {
			return s.Index(0).Interface(), nil
		} else if s.Len() == 0 {
			return nil, nil
		} else {
			return nil, errors.New(`result more than one`)
		}
	}
	return nil, errors.New(`unknown error`)
}

func (gd *GenericDao) Select(intf interface{}) (interface{}, error) {
	return gd.SelectWithExtraQuery(intf, nil)
}

func (gd *GenericDao) SelectWithExtraQuery(intf interface{}, extraQuery *ExtraQueryWrapper) (interface{}, error) {
	return gd.SelectWithExtraQueryAndTx(intf, extraQuery, nil)
}

func (gd *GenericDao) SelectWithExtraQueryAndTx(intf interface{}, extraQuery *ExtraQueryWrapper, tx *sqlx.Tx) (interface{}, error) {
	if reflect.TypeOf(intf).Kind() != reflect.Struct {
		return nil, errors.New(`the interface should be a struct non of pointer`)
	}
	table := gd.entityTableMapping[reflect.TypeOf(intf).Name()]
	if table == `` || strings.TrimSpace(table) == `` {
		return nil, errors.New(`no mapping found for the interface` + reflect.TypeOf(intf).Name())
	}

	if extraQuery == nil {
		extraQuery = &ExtraQueryWrapper{CurrentUsername: ``}
	}
	if extraQuery.Pagination == nil {
		extraQuery.Pagination = &util.Pagination{PageSize: 10, CurrentPage: 0}
	}

	columns, sqlArgs := gd.getValidColumnVal(intf)
	var eqClause sq.And
	for i := 0; i < len(columns); i++ {
		eqClause = append(eqClause, sq.Eq{columns[i]: `?`})
	}
	eqClause = append(eqClause, sq.Eq{`del`: `?`})
	sqlArgs = append(sqlArgs, 0)
	countSql, _, err := sq.Select(`count(1)`).From(table).Where(eqClause).ToSql()
	if err != nil {
		return nil, errors.New(`error occurred when generating the count sql`)
	}
	if tx != nil {
		err = tx.Get(&extraQuery.Pagination.Total, countSql, sqlArgs...)
	} else {
		err = gd.DB.Get(&extraQuery.Pagination.Total, countSql, sqlArgs...)
	}
	if err != nil {
		return nil, err
	}
	mySql, _, err := sq.Select(`*`).From(table).Where(eqClause).
		Offset((extraQuery.Pagination.CurrentPage) * extraQuery.Pagination.PageSize).Limit(extraQuery.Pagination.PageSize).ToSql()
	if err != nil {
		return nil, err
	}
	result := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(intf)), 0, 10)
	x := reflect.New(result.Type())
	x.Elem().Set(result)
	if tx != nil {
		tx = tx.Unsafe()
		err = tx.Select(x.Interface(), mySql, sqlArgs...)
	} else {
		db := gd.DB.Unsafe()
		err = db.Select(x.Interface(), mySql, sqlArgs...)
	}
	return x.Interface(), err
}

func (gd *GenericDao) TransferToSelectBuilder(intf interface{}, extraQuery *ExtraQueryWrapper) (sq.SelectBuilder, []interface{}) {
	table := gd.entityTableMapping[reflect.TypeOf(intf).Name()]
	columns, sqlArgs := gd.getValidColumnVal(intf)
	var and sq.And
	for i := 0; i < len(columns); i++ {
		and = append(and, sq.Eq{columns[i]: `?`})
	}
	and = append(and, sq.Eq{`del`: `?`})
	sqlArgs = append(sqlArgs, 0)
	extraAnd, extraQueryParams, err := gd.addExtraQueryToAnd(intf, extraQuery)
	if err != nil {
		panic(err)
	}
	and = append(and, extraAnd...)
	sqlArgs = append(sqlArgs, extraQueryParams...)
	selectBuilder := sq.Select(`*`).From(table).Where(and)
	return selectBuilder, sqlArgs
}


func (gd *GenericDao) addExtraQueryToAnd(intf interface{}, extraQuery *ExtraQueryWrapper) (sq.And, []interface{}, error){
	var extraAnd sq.And
	currentTable := reflect.TypeOf(intf).Name()
	currentFieldMapping := gd.entityFieldMapping[currentTable]
	for currentFieldMapping == nil || len(currentFieldMapping) == 0{
		return nil, nil, errors.New(`can't find fields mapping for the entity ` + currentTable)
	}
	var values []interface{}
	if extraQuery != nil && extraQuery.Query != nil {
		for i := 0; i < len(extraQuery.Query.And); i++ {
			currentOperator := strings.TrimSpace(extraQuery.Query.And[i].Operator)
			currentJSONFields := strings.TrimSpace(extraQuery.Query.And[i].Field)
			currentValue := strings.TrimSpace(extraQuery.Query.And[i].Value)
			var currentTableField string
			for _, fieldInfo := range currentFieldMapping {
				if fieldInfo.JSONField == currentJSONFields {
					currentTableField = fieldInfo.TableField
					break
				}
			}
			if currentTable == `` {
				return nil, nil, errors.New(`can't find field mapping for the entity and the field ` + currentTable + ` , ` + currentTableField )
			}
			if currentOperator == `eq` || currentOperator == `=` {
				extraAnd = append(extraAnd, sq.Eq{currentTableField: `?`})
			} else if currentOperator == `gt` || currentOperator == `>` {
				extraAnd = append(extraAnd, sq.Gt{currentTableField: `?`})
			} else if currentOperator == `lt` || currentOperator == `<` {
				extraAnd = append(extraAnd, sq.Lt{currentTableField: `?`})
			} else if currentOperator == `gte` || currentOperator == `>=` {
				extraAnd = append(extraAnd, sq.GtOrEq{currentTableField: `?`})
			} else if currentOperator == `lte` || currentOperator == `>=` {
				extraAnd = append(extraAnd, sq.LtOrEq{currentTableField: `?`})
			} else {
				return nil, nil, errors.New(`unrecognised operator: ` + currentOperator)
			}
			values = append(values, currentValue)
		}
	}
	return extraAnd, values, nil
}



func (gd *GenericDao) Update(intf interface{}) (sql.Result, error) {
	return gd.UpdateWithExtraQuery(intf, nil)
}

func (gd *GenericDao) UpdateWithExtraQuery(intf interface{}, extraQueryWrapper *ExtraQueryWrapper) (sql.Result, error) {
	//tableName := entityTableMapping[reflect.TypeOf(intf).String()]
	return gd.UpdateWithExtraQueryWithTx(intf, extraQueryWrapper, nil)
}

func (gd *GenericDao) UpdateWithExtraQueryWithTx(intf interface{}, extraQueryWrapper *ExtraQueryWrapper, tx *sqlx.Tx) (sql.Result, error) {
	//tableName := entityTableMapping[reflect.TypeOf(intf).String()]
	if reflect.TypeOf(intf).Kind() != reflect.Struct {
		panic(`the interface should be a struct non of pointer`)
	}
	table := gd.entityTableMapping[reflect.TypeOf(intf).Name()]

	columns, sqlArgs := gd.getValidColumnVal(intf)
	setMap := sq.Eq{}
	var id interface{}
	for i := 0; i < len(columns); i++ {
		if columns[i] == `id` {
			id = sqlArgs[i]
			continue
		}
		setMap[columns[i]] = sqlArgs[i]
	}
	for i := 0; i < len(extraQueryWrapper.NullFields); i++ {
		setMap[extraQueryWrapper.NullFields[i]] = null.Int{}
	}
	if extraQueryWrapper != nil && extraQueryWrapper.CurrentUsername != `` {
		setMap[FIXED_COLUMN_UPDATE_BY] = extraQueryWrapper.CurrentUsername
	}
	if id == nil {
		panic(`no identifier found`)
	}
	sqlQuery, args, err := sq.Update(table).SetMap(setMap).Where("id = ? and del = ?", id, 0).ToSql()
	if err != nil {
		return nil, errors.New(`can't generate sql from builder`)
	}
	var result sql.Result
	var stmt *sqlx.Stmt
	if tx != nil {
		stmt, err = tx.Preparex(sqlQuery)
	} else {
		stmt, err = gd.DB.Preparex(sqlQuery)
	}
	if err != nil {
		return nil, err
	}
	result, err = stmt.Exec(args...)
	return result, err

}

func (gd *GenericDao) Insert(interf interface{}) (interface{}, error) {
	return gd.InsertWithExtraQuery(interf, nil)
}

func (gd *GenericDao) InsertWithExtraQuery(interf interface{}, extraQueryWrapper *ExtraQueryWrapper) (interface{}, error) {
	return gd.InsertWithExtraQueryAndTx(interf, extraQueryWrapper, nil)
}

func (gd *GenericDao) InsertWithExtraQueryAndTx(interf interface{}, extraQueryWrapper *ExtraQueryWrapper, tx *sqlx.Tx) (interface{}, error) {
	if reflect.TypeOf(interf).Kind() != reflect.Struct {
		panic(`the interface should be a struct non of pointer`)
	}
	table := gd.entityTableMapping[reflect.TypeOf(interf).Name()]
	columns, args := gd.getValidColumnVal(interf)
	var id interface{}
	for i := 0; i < len(columns); i++ {
		if columns[i] == FIXED_COLUMN_ID {
			id = args[i]
			break
		}
	}
	if id != nil {
		panic(`auto generate id, shouldn't set a val`)
	}
	if extraQueryWrapper != nil && extraQueryWrapper.CurrentUsername != `` {
		columns = append(columns, FIXED_COLUMN_CREATE_BY)
		args = append(args, extraQueryWrapper.CurrentUsername)
	}

	sqlQuery, sqlArgs, err := sq.Insert(table).Columns(columns...).Values(args...).ToSql()
	if err != nil {
		panic(err)
	}
	var result sql.Result
	var stmt *sqlx.Stmt
	if tx != nil {
		stmt, err = tx.Preparex(sqlQuery)
	} else {
		stmt, err = gd.DB.Preparex(sqlQuery)
	}
	if err != nil {
		return nil, err
	}
	result, err = stmt.Exec(sqlArgs...)

	if err != nil {
		panic(err)
	}
	if affectedRow, err := result.RowsAffected(); err != nil || affectedRow <= 0 {
		panic(`no row affected`)
	}
	//get the result from db then
	if insertedId, err := result.LastInsertId(); err == nil {
		newInterface := util.CreateObjFromInterface(interf)

		if tx != nil {
			err = tx.Get(newInterface.Interface(), `select * from `+table+` where id = ?`, insertedId)
		} else {
			err = gd.DB.Get(newInterface.Interface(), `select * from `+table+` where id = ?`, insertedId)
		}

		if err != nil {
			fmt.Println(err)
			return nil, err
		} else {
			return newInterface.Interface(), err
		}
	}
	return nil, err
}

// logical delete
func (gd *GenericDao) Delete(intf interface{}) error {
	return gd.DeleteWithExtraQuery(intf, nil)
}

func (gd *GenericDao) DeleteWithExtraQuery(intf interface{}, extraQueryWrapper *ExtraQueryWrapper) error {
	return gd.DeleteWithExtraQueryAndTx(intf, extraQueryWrapper, nil)
}

func (gd *GenericDao) DeleteWithExtraQueryAndTx(intf interface{}, extraQueryWrapper *ExtraQueryWrapper, tx *sqlx.Tx) error {
	if reflect.TypeOf(intf).Kind() != reflect.Struct {
		return errors.New(`the interface should be a struct non of pointer`)
	}
	table := gd.entityTableMapping[reflect.TypeOf(intf).Name()]
	columns, args := gd.getValidColumnVal(intf)
	var id interface{}
	for i := 0; i < len(columns); i++ {
		if columns[i] == FIXED_COLUMN_ID {
			id = args[i]
			break
		}
	}
	if id == nil {
		panic(`no identifier found`)
	}
	updateBuilder := sq.Update(table).Set(`del`, 1)
	if extraQueryWrapper != nil && extraQueryWrapper.CurrentUsername != `` {
		updateBuilder.Set(FIXED_COLUMN_UPDATE_BY, extraQueryWrapper.CurrentUsername)
	}
	var result sql.Result
	sql, queryArgs, err := updateBuilder.Where(`id = ?`, id).ToSql()
	if err != nil {
		return errors.New(`some thing wrong when generating the sql`)
	}
	if tx != nil {
		result, err = tx.Exec(sql, queryArgs...)
	} else {
		result, err = gd.DB.Exec(sql, queryArgs...)
	}

	if err != nil {
		return err
	}
	affectedRow, err := result.RowsAffected()
	if affectedRow <= 0 {
		panic(`operation failed`)
	}
	return err
}

func (gd *GenericDao) getValidColumnVal(intf interface{}) ([]string, []interface{}) {
	var columns []string
	var values []interface{}
	var crtActualVal interface{}
	fieldCount := reflect.TypeOf(intf).NumField()
	for i := 0; i < fieldCount; i++ {
		fieldHasValue := false
		currentField := reflect.TypeOf(intf).Field(i).Type.String()
		currentFieldVal := reflect.ValueOf(intf).Field(i)
		switch currentField {
		case `null.Int`:
			strVal := currentFieldVal.Interface().(null.Int)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.Int64
		case `null.String`:
			strVal := currentFieldVal.Interface().(null.String)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.String
		case `null.Float`:
			strVal := currentFieldVal.Interface().(null.Float)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.Float64
		case `null.Time`:
			strVal := currentFieldVal.Interface().(null.Time)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.Time
		case `null.Bool`:
			strVal := currentFieldVal.Interface().(null.Bool)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.Bool
		case `util.MyNullTime`:
			strVal := currentFieldVal.Interface().(util.MyNullTime)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.Time
		default:
			println(`can't find`)
		}
		if fieldHasValue {
			crtColumnName := reflect.TypeOf(intf).Field(i).Tag.Get("db")
			if crtColumnName != `` {
				columns = append(columns, crtColumnName)
			} else {
				columns = append(columns, strings.ToLower(reflect.TypeOf(intf).Field(i).Name))
			}
			values = append(values, crtActualVal)
		}
	}
	return columns, values
}
