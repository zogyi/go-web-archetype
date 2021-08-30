package go_web_archetype

import (
	"database/sql"
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/***REMOVED***/go-web-archetype/util"
	"go.uber.org/zap"
	"gopkg.in/guregu/null.v3"
	"reflect"
	"strings"
	"time"
)
type Operation string

const(
	Insert Operation = `insert`
	Update Operation = `update`
	Delete Operation = `delete`
	Select Operation = `select`
)

type CommonFields struct {
	Id         null.Int        `json:"id" archType:"primaryKey"`
	CreateTime util.MyNullTime `json:"create_time" db:"create_time" archType:"autoFill"`
	CreatorBy  null.String     `json:"create_by" db:"create_by" archType:"autoFill"`
	UpdateTime util.MyNullTime `json:"update_time" db:"update_time" archType:"autoFill"`
	UpdateBy   null.String     `json:"update_by" db:"update_by" archType:"autoFill"`
	Del        null.Bool       `json:"-" db:"del" archType:"autoFill"`
}

//TODO: 1. extract the field and columns mapping and save into a map
type QueryItem struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

type QueryWrapper struct {
	And   []QueryItem       `json:"and"`
	Group map[string]string `json:"group"`
}

type ExtraQueryWrapper struct {
	CurrentUsername string
	Pagination      *util.Pagination
	Query           *QueryWrapper
	NullFields      []string
}

func NewDefaultExtraQueryWrapper() *ExtraQueryWrapper {
	return &ExtraQueryWrapper{Pagination: &util.Pagination{PageSize: 10, CurrentPage: 0}, Query: &QueryWrapper{}}
}

const (
	FIXED_COLUMN_ID     string = `id`
	FixedColumnCreateBy string = `create_by`
	FixedColumnUpdateBy string = `update_by`
	TagArchType         string = `archType`
	TagPrimaryKey       string = `primaryKey`
	TagAutoFilled       string = `autoFill`
)

type fieldInfo struct {
	Field        string
	JSONField    string
	TableField   string
	Type         string
	IsPrimaryKey bool
	AutoFilled   bool
}

type GenericDao struct {
	db                 *sqlx.DB
	bondEntities       []interface{}                    //所有的entity
	entityTableMapping map[string]string                //entity与table名字之间的映射
	entityFieldMapping map[string]map[string]*fieldInfo //entity所有的表单的映射

	//自定义类型如下
	customType             []interface{}                    //用户自定义的类型
	//customTypeFieldMapping map[string]map[string]*fieldInfo //自定义类型的字段
	//commonFields 	   util.CommonFields
}

func NewGenericDao(db *sqlx.DB) *GenericDao {
	if db == nil {
		panic(`the pointer of database is nil`)
	}
	if err := db.Ping(); err != nil {
		fmt.Println(err)
		panic(`can't connect to the database`)
	}
	return &GenericDao{db: db}
}

func NewDaoWithDefaultCustomerType(db *sqlx.DB) *GenericDao {
	dao := NewGenericDaoWithCustomerType(db, CommonFields{})
	return dao
}

func NewGenericDaoWithCustomerType(db *sqlx.DB, types ...interface{}) *GenericDao {
	dao := NewGenericDao(db)
	dao.AddCustomType(types...)
	return dao
}

func (gd *GenericDao) AddCustomType(types ...interface{}) *GenericDao {
	//reflect.TypeOf(types).Name()
	for i := 0; i < len(types); i++ {
		crtType := reflect.TypeOf(types[i])
		if crtType.Kind() != reflect.Struct {
			panic(`wrong type of the type, should be a struct`)
		}
		//添加到自定义类型中
		currentTypeName := crtType.Name()
		crtTypeFieldMap := make(map[string]*fieldInfo)
		for k := 0; k < crtType.NumField(); k++ {
			crtField := crtType.Field(k)
			crtTypeFieldMap[crtField.Name] = getFieldInfo(crtField)
		}
		if gd.entityFieldMapping == nil {
			gd.entityFieldMapping = make(map[string]map[string]*fieldInfo)
		}
		gd.entityFieldMapping[currentTypeName] = crtTypeFieldMap
	}
	gd.customType = append(gd.customType, types...)
	return gd
}

func (gd *GenericDao) containCustomType(fieldType reflect.Type) bool {
	for i := 0; i < len(gd.customType); i++ {
		if reflect.TypeOf(gd.customType[i]) == fieldType {
			return true
		}
	}
	return false
}

func (gd *GenericDao) DB() *sqlx.DB {
	return gd.db
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
		if gd.containCustomType(currentField.Type) {
			if customTypeFields, ok := gd.entityFieldMapping[currentField.Name]; ok {
				for k, v := range customTypeFields {
					fieldsMapping[k] = v
				}
			} else {
				panic(`can't found the required type`)
			}
		} else {
			fieldsMapping[currentField.Name] = getFieldInfo(currentField)

		}
	}
	gd.entityFieldMapping[crtIrf.Name()] = fieldsMapping
	gd.bondEntities = append(gd.bondEntities, interf)
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
	sqlTag := field.Tag.Get(TagArchType)
	var isPrimaryKey bool
	var autoFill bool
	if strings.TrimSpace(sqlTag) != `` {
		sqlItems := strings.Split(strings.TrimSpace(sqlTag), `,`)
		if util.StringInSlice(TagPrimaryKey, sqlItems) {
			isPrimaryKey = true
		}
		if util.StringInSlice(TagAutoFilled, sqlItems) {
			autoFill = true
		}
	}
	zap.L().Debug(fmt.Sprint(fieldInfo{JSONField: jsonField, TableField: tableFiled, Type: field.Type.Name(), IsPrimaryKey: isPrimaryKey, AutoFilled: autoFill}))
	return &fieldInfo{JSONField: jsonField, TableField: tableFiled, Type: field.Type.Name(), IsPrimaryKey: isPrimaryKey, AutoFilled: autoFill}
}

func (gd *GenericDao) GetById(intf interface{}, id uint64, force bool) (interface{}, error) {
	objType := reflect.TypeOf(intf)
	result := reflect.New(objType)
	result.Elem().FieldByName(`Id`).Set(reflect.ValueOf(null.IntFrom(int64(id))))
	return gd.Get(result.Elem().Interface(), NewDefaultExtraQueryWrapper(), force)
}

//GetOne return the pointer of the result object
func (gd *GenericDao) Get(intf interface{}, extraQuery *ExtraQueryWrapper, force bool) (interface{}, error) {
	return gd.GetOneWithTx(intf, extraQuery, nil, force)
}

func (gd *GenericDao) GetOneWithTx (intf interface{}, extraQuery *ExtraQueryWrapper, tx *sqlx.Tx, force bool) (interface{}, error) {
	sqlBuilder, sqlArgs := gd.TransferToSelectBuilder(intf, extraQuery)
	sqlQuery, _, err := sqlBuilder.ToSql()
	if err != nil {
		return nil, err
	}
	resultType := reflect.TypeOf(intf)
	resultVal := reflect.New(resultType)
	zap.L().Sugar().Debugf("SQL: %s, Arguments: %s", sqlQuery, sqlArgs)
	if tx != nil {
		err = tx.Get(resultVal.Interface(), sqlQuery, sqlArgs...)
	} else {
		err = gd.DB().Get(resultVal.Interface(), sqlQuery, sqlArgs...)
	}
	if err != nil{
		if !force && err == sql.ErrNoRows{
			return nil, nil
		}
		return nil, err
	}
	return resultVal.Interface(), nil

}

func (gd *GenericDao) Select(intf interface{}) (interface{}, error) {
	return gd.SelectWithExtraQuery(intf, nil)
}

func (gd *GenericDao) SelectWithExtraQuery(intf interface{}, extraQuery *ExtraQueryWrapper) (interface{}, error) {
	return gd.SelectWithExtraQueryAndTx(intf, extraQuery, nil)
}

func (gd *GenericDao) SelectWithExtraQueryAndTx(intf interface{}, extraQuery *ExtraQueryWrapper, tx *sqlx.Tx) (interface{}, error) {
	if extraQuery == nil {
		extraQuery = NewDefaultExtraQueryWrapper()
	}
	if reflect.TypeOf(intf).Kind() != reflect.Struct {
		return nil, errors.New(`the interface should be a struct non of pointer`)
	}
	table := gd.entityTableMapping[reflect.TypeOf(intf).Name()]
	if table == `` || strings.TrimSpace(table) == `` {
		return nil, errors.New(`no mapping found for the interface` + reflect.TypeOf(intf).Name())
	}

	sqlBuilder, sqlArgs := gd.TransferToSelectBuilder(intf, extraQuery)
	countSql, _, err := sq.Select("count(*)").FromSelect(sqlBuilder, `t`).ToSql()
	if err != nil {
		return nil, errors.New(`error occurred when generating the count sql`)
	}
	zap.L().Sugar().Debugf("SQL: %s, Arguments: %s", countSql, sqlArgs)
	if tx != nil {
		err = tx.Get(&extraQuery.Pagination.Total, countSql, sqlArgs...)
	} else {
		err = gd.db.Get(&extraQuery.Pagination.Total, countSql, sqlArgs...)
	}
	if err != nil {
		return nil, err
	}
	mySql, _, err := sqlBuilder.Offset((extraQuery.Pagination.CurrentPage) * extraQuery.Pagination.PageSize).Limit(extraQuery.Pagination.PageSize).ToSql()
	if err != nil {
		return nil, err
	}
	result := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(intf)), 0, 10)
	x := reflect.New(result.Type())
	x.Elem().Set(result)
	zap.L().Sugar().Debugf("SQL: %s, Arguments: %s", mySql, sqlArgs)
	if tx != nil {
		tx = tx.Unsafe()
		err = tx.Select(x.Interface(), mySql, sqlArgs...)
	} else {
		db := gd.db.Unsafe()
		err = db.Select(x.Interface(), mySql, sqlArgs...)
	}
	return x.Interface(), err
}

func (gd *GenericDao) TransferToSelectBuilder(intf interface{}, extraQuery *ExtraQueryWrapper) (sq.SelectBuilder, []interface{}) {
	if extraQuery == nil {
		extraQuery = NewDefaultExtraQueryWrapper()
	}
	table := gd.entityTableMapping[reflect.TypeOf(intf).Name()]
	fields, values  := gd.getValidColumnVal(intf, Select, extraQuery)

	var and sq.And
	var sqlArgs []interface{}
	for i := 0; i < len(fields); i++ {
		and = append(and, sq.Eq{fields[i].TableField: `?`})
		sqlArgs = append(sqlArgs, values[i])
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

func (gd *GenericDao) addExtraQueryToAnd(intf interface{}, extraQuery *ExtraQueryWrapper) (sq.And, []interface{}, error) {
	if extraQuery == nil {
		extraQuery = NewDefaultExtraQueryWrapper()
	}
	var extraAnd sq.And
	currentEntity := reflect.TypeOf(intf).Name()
	currentFieldMapping := gd.entityFieldMapping[currentEntity]
	for currentFieldMapping == nil || len(currentFieldMapping) == 0 {
		return nil, nil, errors.New(`can't find fields mapping for the entity ` + currentEntity)
	}
	var values []interface{}
	if extraQuery != nil && extraQuery.Query != nil {
		for i := 0; i < len(extraQuery.Query.And); i++ {
			var currentValue interface{}
			currentOperator := strings.ToLower(strings.TrimSpace(extraQuery.Query.And[i].Operator))
			currentJSONFields := strings.TrimSpace(extraQuery.Query.And[i].Field)
			currentValue = extraQuery.Query.And[i].Value
			var currentTableField string
			for _, fieldInfo := range currentFieldMapping {
				if fieldInfo.JSONField == currentJSONFields {
					currentTableField = fieldInfo.TableField
					break
				}
			}
			if strings.TrimSpace(currentEntity) == `` || strings.TrimSpace(currentTableField) == `` {
				return nil, nil, errors.New(fmt.Sprintf(`can't find field mapping for the entity '%v' and the field '%v'`, currentEntity, currentJSONFields))
			}

			if currentOperator == `in` {
				queryVal := reflect.ValueOf(currentValue)
				if queryVal.Kind() == reflect.String {
					currentValue = strings.Split(currentValue.(string), `,`)
				}
				inParams := util.InterfaceSlice(currentValue)
				//if current value is string, then convert it to the string and split the string with comma
				extraAnd = append(extraAnd, sq.Eq{currentTableField: inParams})
				values = append(values, inParams...)
				continue //TODO: investigation, find a better way to unify the query param, solve the place holder can't generate the params for it
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
			} else if currentOperator == `like` {
				extraAnd = append(extraAnd, sq.Like{currentTableField: `?`})
				currentValue = `%` + fmt.Sprint(currentValue) + `%`
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

//update remove the common fields
func (gd *GenericDao) UpdateWithExtraQueryWithTx(intf interface{}, extraQueryWrapper *ExtraQueryWrapper, tx *sqlx.Tx) (sql.Result, error) {
	//tableName := entityTableMapping[reflect.TypeOf(intf).String()]
	if extraQueryWrapper == nil {
		extraQueryWrapper = NewDefaultExtraQueryWrapper()
	}

	if reflect.TypeOf(intf).Kind() != reflect.Struct {
		panic(`the interface should be a struct non of pointer`)
	}

	table := gd.entityTableMapping[reflect.TypeOf(intf).Name()]
	fields, values := gd.getValidColumnVal(intf, Update, extraQueryWrapper)
	setMap := sq.Eq{}
	var args  []interface{}
	for i := 0; i < len(fields); i++ {
		if !fields[i].AutoFilled && !fields[i].IsPrimaryKey{
			setMap[fields[i].TableField] = values[i]
		}
		if fields[i].IsPrimaryKey {
			args = append(args, values[i])
		}
	}

	for i := 0; i < len(extraQueryWrapper.NullFields); i++ {
		setMap[extraQueryWrapper.NullFields[i]] = null.Int{}
	}
	if extraQueryWrapper != nil && extraQueryWrapper.CurrentUsername != `` {
		setMap[FixedColumnUpdateBy] = extraQueryWrapper.CurrentUsername
	}
	if len(args) < 1 {
		panic(`no identifier found`)
	}
	args = append(args, 0)
	sqlQuery, args, err := sq.Update(table).SetMap(setMap).Where("id = ? and del = ?", args...).ToSql()
	//zap.L().Sugar().Debugf("SQL: %s, Arguments: %s", sql, sqlArgs)
	zap.L().Sugar().Debugf("SQL: %s, Arguments: %s", sqlQuery, args)
	if err != nil {
		return nil, errors.New(`can't generate sql from builder`)
	}
	var result sql.Result
	var stmt *sqlx.Stmt
	if tx != nil {
		stmt, err = tx.Preparex(sqlQuery)
	} else {
		stmt, err = gd.db.Preparex(sqlQuery)
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
	if extraQueryWrapper == nil {
		extraQueryWrapper = NewDefaultExtraQueryWrapper()
	}
	if reflect.TypeOf(interf).Kind() != reflect.Struct {
		panic(`the interface should be a struct non of pointer`)
	}
	table, ok := gd.entityTableMapping[reflect.TypeOf(interf).Name()]
	if !ok {
		return nil, errors.New(`can't find the configuration for the type of ` + reflect.TypeOf(interf).Name())
	}

	fields, values  := gd.getValidColumnVal(interf, Insert, extraQueryWrapper)
	var columns []string
	for _, item := range fields {
		columns = append(columns, item.TableField)
	}
	sqlQuery, sqlArgs, err := sq.Insert(table).Columns(columns...).Values(values...).ToSql()
	if err != nil {
		panic(err)
	}
	var result sql.Result
	var stmt *sqlx.Stmt
	zap.L().Sugar().Debugf("SQL: %s, Arguments: %s", sqlQuery, values)
	if tx != nil {
		stmt, err = tx.Preparex(sqlQuery)
	} else {
		stmt, err = gd.db.Preparex(sqlQuery)
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

	if insertedId, err := result.LastInsertId(); err == nil {
		if _, ok := reflect.TypeOf(interf).FieldByName(`Id`); ok {
			result := reflect.New(reflect.TypeOf(interf))
			result.Elem().Set(reflect.ValueOf(interf))
			result.Elem().FieldByName(`Id`).Set(reflect.ValueOf(null.IntFrom(insertedId)))
			return result, nil
		}
	}
	//get the result from db then TODO: if we need to remote the query the result for it
	//if insertedId, err := result.LastInsertId(); err == nil {
	//	newInterface := util.CreateObjFromInterface(interf)
	//
	//	if tx != nil {
	//		//TODO: prepare statement
	//		err = tx.Get(newInterface.Interface(), `select * from ` + table +` where id = ?`, insertedId)
	//	} else {
	//		err = gd.db.Get(newInterface.Interface(), `select * from ` + table + ` where id = ?`, insertedId)
	//	}
	//
	//	if err != nil {
	//		return nil, err
	//	} else {
	//		return newInterface.Interface(), err
	//	}
	//}
	return interf, err
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
	if extraQueryWrapper == nil {
		extraQueryWrapper = NewDefaultExtraQueryWrapper()
	}
	table := gd.entityTableMapping[reflect.TypeOf(intf).Name()]
	fields, values  := gd.getValidColumnVal(intf, Delete, extraQueryWrapper)
	zap.L().Sugar().Debugf(`the result is %s, %s`, fmt.Sprint(fields), fmt.Sprint(values))
	var args []interface{}
	for i := 0; i < len(fields); i++ {
		if fields[i].IsPrimaryKey {
			args = append(args, values[i])
		}
	}
	if len(args) < 1 {
		panic(`no identifier found`)
	}
	updateBuilder := sq.Update(table).Set(`del`, 1)
	if extraQueryWrapper != nil && extraQueryWrapper.CurrentUsername != `` {
		updateBuilder.Set(FixedColumnUpdateBy, extraQueryWrapper.CurrentUsername)
	}
	var result sql.Result
	sql, queryArgs, err := updateBuilder.Where(`id = ?`, args...).ToSql()

	if err != nil {
		return errors.New(`some thing wrong when generating the sql`)
	}
	zap.L().Sugar().Debugf("SQL: %s, Arguments: %s", sql, args)
	if tx != nil {
		result, err = tx.Exec(sql, queryArgs...)
	} else {
		result, err = gd.db.Exec(sql, queryArgs...)
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

func (gd *GenericDao) getValidColumnVal(intf interface{}, operation Operation, extraQueryWrapper *ExtraQueryWrapper) ([]fieldInfo, []interface{}) {
	var columns []fieldInfo
	var values []interface{}
	var crtActualVal interface{}
	fieldCount := reflect.TypeOf(intf).NumField()
	typeName := reflect.TypeOf(intf).Name()
	fieldsConfig, ok := gd.entityFieldMapping[typeName]
	if !ok {
		panic(`can't find the config for the type of ` + typeName)
	}
	for i := 0; i < fieldCount; i++ {
		crtField := reflect.TypeOf(intf).Field(i)
		crtVal := reflect.ValueOf(intf).Field(i)

		if gd.containCustomType(crtField.Type) {
			crtVal := crtVal.Interface()
			customColumns, customValues := gd.getValidColumnVal(crtVal, operation, extraQueryWrapper)
			columns = append(columns, customColumns...)
			values = append(values, customValues...)
			//gd.getValidColumnVal()
			continue
		}
		fieldHasValue := false
		currentField := crtField.Name
		fConfig, ok := fieldsConfig[currentField]
		if !ok {
			panic(`can't find the config for the type of ` + typeName + ` field:` + currentField)
		}
		if fConfig.AutoFilled {
			//TODO: set the value at this place for the insert, update and delete operation
			if operation == Insert {
				if strings.ToLower(fConfig.Field) == `createby` || strings.ToLower(fConfig.TableField) == `create_by` {
					columns = append(columns, *fConfig)
					values = append(values, extraQueryWrapper.CurrentUsername)
				}
				if strings.ToLower(fConfig.Field) == `createtime` || strings.ToLower(fConfig.TableField) == `create_time` {
					fmt.Println(`create_time`)
					columns = append(columns, *fConfig)
					values = append(values, null.TimeFrom(time.Now()))
				}
			}
			if operation == Delete || operation == Update {
				if strings.ToLower(fConfig.Field) == `updateby` || strings.ToLower(fConfig.TableField) == `update_by` {
					columns = append(columns, *fConfig)
					values = append(values, extraQueryWrapper.CurrentUsername)
				}
				if strings.ToLower(fConfig.Field) == `updatetime` || strings.ToLower(fConfig.TableField) == `update_time` {
					columns = append(columns, *fConfig)
					values = append(values, null.TimeFrom(time.Now()))
				}
			}
			continue
		}
		switch crtField.Type {
		case reflect.TypeOf(null.Int{}):
			strVal := crtVal.Interface().(null.Int)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.Int64
		case reflect.TypeOf(null.String{}):
			strVal := crtVal.Interface().(null.String)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.String
		case reflect.TypeOf(null.Float{}):
			strVal := crtVal.Interface().(null.Float)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.Float64
		case reflect.TypeOf(null.Time{}):
			strVal := crtVal.Interface().(null.Time)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.Time
		case reflect.TypeOf(null.Bool{}):
			strVal := crtVal.Interface().(null.Bool)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.Bool
		case reflect.TypeOf(util.MyNullTime{}):
			strVal := crtVal.Interface().(util.MyNullTime)
			fieldHasValue = strVal.Valid
			crtActualVal = strVal.Time
		default:
			zap.L().Info(`can't find the configuration`)
		}
		if fieldHasValue {
			columns = append(columns, *fConfig)
			values = append(values, crtActualVal)
		}
	}
	return columns, values
}
