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
type QueryOperator string

const (
	QPEq     QueryOperator = `eq`
	QPEqSmb  QueryOperator = `=`
	QPGt     QueryOperator = `gt`
	QPGtSmb  QueryOperator = `>`
	QPLt     QueryOperator = `lt`
	QPLtSmb  QueryOperator = `<`
	QPGte    QueryOperator = `gte`
	QPGteSmb QueryOperator = `>=`
	QPLte    QueryOperator = `lte`
	QPLteSmb QueryOperator = `<=`
	QPLike   QueryOperator = `like`
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
	Field    string        `json:"field"`
	Operator QueryOperator `json:"operator"`
	Value    interface{}   `json:"value"`
}

type QueryWrapper struct {
	And   []QueryItem       `json:"and"`
	Group map[string]string `json:"group"`
}

type ExtraQueryWrapper struct {
	CurrentUsername string
	Pagination      *util.Pagination
	Query           *QueryWrapper
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
	if crtIrf.Kind() != reflect.Struct {
		panic(`only struct is ok`)
	}
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
		if !isPrimaryKey && !autoFill {
			panic(fmt.Sprintf(`unrecognized tag value: <%s>, the right format looks like this: "%s:%s,%s"`, sqlTag, TagArchType, TagPrimaryKey, TagAutoFilled))
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
	sqlBuilder := gd.TransferToSelectBuilder(intf, extraQuery)
	sqlQuery, sqlArgs, err := sqlBuilder.ToSql()
	if err != nil {
		return nil, err
	}
	daoExecutor := daoExecutor{gd.db, tx}
	return daoExecutor.get(sqlQuery, sqlArgs, reflect.TypeOf(intf))
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

	executor := daoExecutor{DB: gd.db, Tx: tx}
	builder := gd.TransferToSelectBuilder(intf, extraQuery)
	countSql, sqlArgs, err := sq.Select("count(*)").FromSelect(builder, `t1`).ToSql()
	if err != nil {
		return nil, errors.New(`error occurred when generating the count sql`)
	}
	if total, err := executor.get(countSql, sqlArgs, reflect.TypeOf((*uint64)(nil)).Elem()); err != nil {
		return nil, err
	} else {
		extraQuery.Pagination.Total = *total.(*uint64)
	}
	sqlQuery, sqlArgs, err := sq.Select(`*`).FromSelect(builder, `t1`).
		Offset((extraQuery.Pagination.CurrentPage) * extraQuery.Pagination.PageSize).
		Limit(extraQuery.Pagination.PageSize).ToSql()

	if err != nil {
		return nil, err
	}
	return executor.selectList(sqlQuery, sqlArgs, reflect.TypeOf(intf))
}

func (gd *GenericDao) TransferToSelectBuilder(intf interface{}, extraQuery *ExtraQueryWrapper) sq.SelectBuilder {
	if extraQuery == nil {
		extraQuery = NewDefaultExtraQueryWrapper()
	}
	table := gd.entityTableMapping[reflect.TypeOf(intf).Name()]
	eqClause, _ , _ := gd.Validate(intf, Select, extraQuery.CurrentUsername)

	extraAnd, err := gd.addExtraQueryToAnd(intf, extraQuery)
	if err != nil {
		panic(err)
	}
	extraAnd = append(extraAnd, eqClause)
	selectBuilder := sq.Select(`*`).From(table).Where(extraAnd)
	return selectBuilder
}

func (gd *GenericDao) addExtraQueryToAnd(intf interface{}, extraQuery *ExtraQueryWrapper) (sq.And, error) {
	if extraQuery == nil {
		extraQuery = NewDefaultExtraQueryWrapper()
	}
	var extraAnd sq.And
	currentEntity := reflect.TypeOf(intf).Name()
	currentFieldMapping := gd.entityFieldMapping[currentEntity]
	for currentFieldMapping == nil || len(currentFieldMapping) == 0 {
		return nil, errors.New(`can't find fields mapping for the entity ` + currentEntity)
	}
	if extraQuery != nil && extraQuery.Query != nil {
		for i := 0; i < len(extraQuery.Query.And); i++ {
			var currentValue interface{}
			currentOperator := extraQuery.Query.And[i].Operator
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
				return nil, errors.New(fmt.Sprintf(`can't find field mapping for the entity '%v' and the field '%v'`, currentEntity, currentJSONFields))
			}

			if currentOperator == `in` {
				queryVal := reflect.ValueOf(currentValue)
				if queryVal.Kind() == reflect.String {
					currentValue = strings.Split(currentValue.(string), `,`)
				}
				inParams := util.InterfaceSlice(currentValue)
				//if current value is string, then convert it to the string and split the string with comma
				extraAnd = append(extraAnd, sq.Eq{currentTableField: inParams})
				//values = append(values, inParams...)
				continue //TODO: investigation, find a better way to unify the query param, solve the place holder can't generate the params for it
			}

			if currentOperator == QPEq || currentOperator == QPEqSmb {
				extraAnd = append(extraAnd, sq.Eq{currentTableField: currentValue})
			} else if currentOperator == QPGt || currentOperator == QPGtSmb {
				extraAnd = append(extraAnd, sq.Gt{currentTableField: currentValue})
			} else if currentOperator == QPLt || currentOperator == QPGtSmb {
				extraAnd = append(extraAnd, sq.Lt{currentTableField: currentValue})
			} else if currentOperator == QPGte || currentOperator == QPGteSmb {
				extraAnd = append(extraAnd, sq.GtOrEq{currentTableField: currentValue})
			} else if currentOperator == QPLte || currentOperator == QPLteSmb {
				extraAnd = append(extraAnd, sq.LtOrEq{currentTableField: currentValue})
			} else if currentOperator == QPLike {
				currentValue = `%` + fmt.Sprint(currentValue) + `%`
				extraAnd = append(extraAnd, sq.Like{currentTableField: currentValue})
			} else {
				return nil, errors.New(fmt.Sprint(`unrecognised operator: `, currentOperator))
			}
		}
	}
	return extraAnd, nil
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

	eqClause, setMap, _ := gd.Validate(intf, Update, extraQueryWrapper.CurrentUsername)
	//fields, values := gd.getValidColumnVal(returnResult, Update, extraQueryWrapper)
	sqlQuery, args, err := sq.Update(table).SetMap(setMap).Where(eqClause).ToSql()
	if err != nil {
		return nil, err
	}
	executor := daoExecutor{DB: gd.db, Tx: tx}
	return executor.insertOrUpdate(sqlQuery, args)
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
	_, setMap, _  := gd.Validate(interf, Insert, extraQueryWrapper.CurrentUsername)
	sqlQuery, sqlArgs, err := sq.Insert(table).SetMap(setMap).ToSql()
	if err != nil {
		return nil, err
	}
	executor := daoExecutor{DB: gd.db, Tx: tx}
	result, err := executor.insertOrUpdate(sqlQuery, sqlArgs)

	if err != nil {
		return nil, err
	}
	if affectedRow, err := result.RowsAffected(); err != nil || affectedRow <= 0 {
		return nil, errors.New(`no row affected`)
	}

	if insertedId, err := result.LastInsertId(); err == nil {
		if _, ok := reflect.TypeOf(interf).FieldByName(`Id`); ok {
			result := reflect.New(reflect.TypeOf(interf))
			result.Elem().Set(reflect.ValueOf(interf))
			err := util.SetFieldValByName(result.Interface(), `Id`, null.IntFrom(insertedId))
			return result.Interface(), err
		}
	}
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

	eqClause, setMap, _  := gd.Validate(intf, Delete, extraQueryWrapper.CurrentUsername)

	sqlQuery, sqlArgs, err := sq.Update(table).Where(eqClause).SetMap(setMap).ToSql()
	//sqlQuery, queryArgs, err := updateBuilder.Where(whereSql, values...).ToSql()

	if err != nil {
		return errors.New(`some thing wrong when generating the sql`)
	}
	executor := daoExecutor{DB: gd.db, Tx: tx}
	var sqlResult sql.Result
	var rows int64
	if sqlResult, err = executor.insertOrUpdate(sqlQuery, sqlArgs); err == nil {
		if rows, err = sqlResult.RowsAffected(); err == nil && rows <= 0 {
			return errors.New(`no rows effected`)
		}
	}
	return err
}

func (gd *GenericDao)Validate (intf interface{}, operation Operation, executeUser string) (eqClause sq.Eq, setMap map[string]interface{}, primaryKeyValid bool) {
	intfType := reflect.TypeOf(intf)
	intfVal := reflect.ValueOf(intf)
	//whereClause := sq.Eq{}
	//setClause := make(map[string]interface)
	var returnIntf reflect.Value
	if intfType.Kind() == reflect.Struct {
		returnIntf = reflect.New(intfType)
		returnIntf.Elem().Set(intfVal)
	}
	var fieldsConfiguration map[string]*fieldInfo
	var ok bool
	if fieldsConfiguration, ok = gd.entityFieldMapping[intfType.Name()]; !ok {
		panic(`can't find the fields configuration for the struct ` + intfType.Name())
	}
	if strings.TrimSpace(executeUser) == `` {
		executeUser = `system`
	}
	eqClause = make(sq.Eq)
	setMap = make(map[string]interface{})
	for i := 0; i < intfType.NumField(); i++ {
		var filedCfg *fieldInfo
		crtFiledType := intfType.Field(i)
		crtFiledVal := returnIntf.Elem().FieldByName(crtFiledType.Name)
		if gd.containCustomType(crtFiledType.Type) {
			subEqClause, subSetMap, subPrimaryKeyValid := gd.Validate(crtFiledVal.Interface(), operation, executeUser)
			for k, v := range subEqClause {
				eqClause[k] = v
			}
			for k, v := range subSetMap {
				setMap[k] = v
			}
			primaryKeyValid = primaryKeyValid || subPrimaryKeyValid
			continue
		}
		if filedCfg, ok = fieldsConfiguration[crtFiledType.Name]; !ok {
			panic(fmt.Sprintf(`can't find the configuration for the struct %s of field %s`, intfType.Name(), crtFiledType.Name))
		}
		if filedCfg.AutoFilled {
			if (strings.ToLower(filedCfg.TableField) == `update_by` && (operation == Delete || operation == Update)) ||
				(strings.ToLower(filedCfg.TableField) == `create_by` && operation == Insert) {
				setMap[filedCfg.TableField] = null.StringFrom(executeUser)
				continue
			}
			if (strings.ToLower(filedCfg.TableField) == `update_time` && (operation == Delete || operation == Update)) ||
				(strings.ToLower(filedCfg.TableField) == `create_time` && operation == Insert) {
				setMap[filedCfg.TableField] = util.MyNullTime{Time: null.TimeFrom(time.Now())}
				continue
			}
			if strings.ToLower(filedCfg.TableField) == `del` {
				if operation != Insert {eqClause[filedCfg.TableField] = null.BoolFrom(false)}
				if operation == Delete {setMap[filedCfg.TableField] = null.BoolFrom(true)}
				continue
			}
		}
		if filedCfg.IsPrimaryKey {
			primaryKeyValid = !crtFiledVal.IsZero()
			if operation != Insert && primaryKeyValid{
				eqClause[filedCfg.TableField] = crtFiledVal.Interface()
			}
			if operation == Insert && primaryKeyValid && filedCfg.AutoFilled {
				panic(`don't set a value for the primary key when it set as autoFilled`)
			}
			continue
		}
		if !crtFiledVal.IsZero() {
			if operation == Insert || operation == Update {
				setMap[filedCfg.TableField] = crtFiledVal.Interface()
			}
			if operation == Delete || operation == Select {
				eqClause[filedCfg.TableField] = crtFiledVal.Interface()
			}
		}
	}
	if (operation == Delete || operation == Update) && !primaryKeyValid && !gd.containCustomType(intfType) {
		panic(`unsupported query object, should have value for the primary key when execute the update or delete method`)
	}
	return eqClause, setMap, primaryKeyValid
}
