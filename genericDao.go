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

const (
	Insert Operation = `insert`
	Update Operation = `update`
	Delete Operation = `delete`
	Select Operation = `select`
)

type CommonFields struct {
	Id         null.Int        `json:"id" archType:"primaryKey"`
	CreateTime util.MyNullTime `json:"createTime" db:"create_time" archType:"autoFill"`
	CreatorBy  null.String     `json:"createBy" db:"create_by" archType:"autoFill"`
	UpdateTime util.MyNullTime `json:"updateTime" db:"update_time" archType:"autoFill"`
	UpdateBy   null.String     `json:"updateBy" db:"update_by" archType:"autoFill"`
}

type CommonDel struct {
	Del null.Bool `json:"-" db:"del" archType:"autoFill"`
}

//TODO: 1. extract the field and columns mapping and save into a map

type OrderByType string

const (
	ASC  OrderByType = `ASC`
	DESC OrderByType = `DESC`
)

type OrderBy struct {
	JSONFields []string    `json:"fields"`
	columns    []string    `json:"-"`
	OrderType  OrderByType `json:"orderType"`
}

func (ob *OrderBy) setColumn(columns []string) {
	ob.columns = columns
}

func (ob *OrderBy) ToSql() string {
	if ob.columns != nil && len(ob.columns) > 0 && strings.TrimSpace(string(ob.OrderType)) != `` {
		return fmt.Sprintf(`%s %s`, strings.Join(ob.columns, `,`), ob.OrderType)
	}
	return ``
}

type QueryWrapper struct {
	Query   `json:"query"`
	GroupBy []string  `json:"groupBy"`
	OrderBy []OrderBy `json:"orderBy"`
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
	FixedColumnCreateBy string = `create_by`
	FixedColumnUpdateBy string = `update_by`
	FixedColumnDel      string = `del`
	TagArchType         string = `archType`
	TagPrimaryKey       string = `primaryKey`
	TagAutoFilled       string = `autoFill`
)

type fieldInfo struct {
	Field        string
	JSONField    string
	TableField   string
	Type         string
	IsLogicDel   bool
	IsPrimaryKey bool
	AutoFilled   bool
}

type structInfo struct {
	fieldInfos       map[string]fieldInfo //所有的字段集合
	jsonFieldInfos   map[string]fieldInfo //所有的字段集合
	primaryKey       fieldInfo            //主键字段
	autoFilledFields map[string]fieldInfo //自动填充字段
	LogicDel         bool
}

func (strFieldInfo *structInfo) GetColumns() (columns []string) {
	for _, info := range strFieldInfo.fieldInfos {
		columns = append(columns, info.TableField)
	}
	return columns
}

func (strFieldInfo *structInfo) addField(fInfo fieldInfo) {
	if (fieldInfo{}) == fInfo {
		panic(`the param fInfo is null`)
	}
	if strFieldInfo.fieldInfos == nil {
		strFieldInfo.fieldInfos = make(map[string]fieldInfo)
	}
	if strFieldInfo.jsonFieldInfos == nil {
		strFieldInfo.jsonFieldInfos = make(map[string]fieldInfo)
	}
	if strFieldInfo.autoFilledFields == nil {
		strFieldInfo.autoFilledFields = make(map[string]fieldInfo)
	}
	strFieldInfo.fieldInfos[fInfo.Field] = fInfo
	strFieldInfo.jsonFieldInfos[fInfo.JSONField] = fInfo
	if fInfo.IsPrimaryKey {
		if (fieldInfo{} != strFieldInfo.primaryKey) && strFieldInfo.primaryKey.TableField != fInfo.TableField {
			panic(fmt.Sprintf(`the field set as primary key can't more than 1, the existing primary key is %s and the new primary key is %s`, strFieldInfo.primaryKey.TableField, fInfo.TableField))
		}
		strFieldInfo.primaryKey = fInfo
	}
	if fInfo.AutoFilled {
		strFieldInfo.autoFilledFields[fInfo.Field] = fInfo
	}
	if fInfo.IsLogicDel {
		strFieldInfo.LogicDel = true
	}
}

type entitiesInfo map[string]structInfo

type GenericDao struct {
	db                 *sqlx.DB
	bondEntities       []interface{}     //所有的entity
	entityTableMapping map[string]string //entity与table名字之间的映射
	entitiesInfos      entitiesInfo      //entity field所有的表单的映射
	//tablePrimaryKey    map[string]*fieldInfo       //table's primary key mapping
	//自定义类型如下
	customType []interface{} //用户自定义的类型
	//customTypeFieldMapping map[string]map[string]*fieldInfo //自定义类型的字段
	//commonFields 	   util.CommonFields
}

func (gd *GenericDao) getFieldInfo(structName string, jsonName string) (fieldInfo, bool) {
	if structInfo, exist := gd.entitiesInfos[structName]; exist {
		for _, fieldInfo := range structInfo.fieldInfos {
			if fieldInfo.JSONField == jsonName {
				return fieldInfo, true
			}
		}
	}
	return fieldInfo{}, false
}

func (gd *GenericDao) GetColumns(entity string) (columns []string, exist bool) {
	if fieldsInfo, exist := gd.entitiesInfos[entity]; exist {
		return fieldsInfo.GetColumns(), true
	}
	return columns, false
}

func (gd *GenericDao) GetTable(entity string) (string, bool) {
	if table, exist := gd.entityTableMapping[entity]; exist {
		return table, true
	}
	return ``, false
}

func NewGenericDao(db *sqlx.DB) *GenericDao {
	if db == nil {
		panic(`the pointer of database is nil`)
	}
	if err := db.Ping(); err != nil {
		zap.L().Error(`can't connect to the database`)
		panic(`can't connect to the database`)
	}
	return &GenericDao{db: db}
}

func NewDaoWithDefaultCustomerType(db *sqlx.DB) *GenericDao {
	dao := NewGenericDaoWithCustomerType(db, CommonFields{}, CommonDel{})
	return dao
}

func NewGenericDaoWithCustomerType(db *sqlx.DB, types ...interface{}) *GenericDao {
	dao := NewGenericDao(db)
	dao.AddCustomType(types...)
	return dao
}

func (gd *GenericDao) AddCustomType(types ...interface{}) *GenericDao {
	//reflect.TypeOf(types).Name()
	if gd.entitiesInfos == nil {
		gd.entitiesInfos = entitiesInfo{}
	}
	for i := 0; i < len(types); i++ {
		crtType := reflect.TypeOf(types[i])
		if crtType.Kind() != reflect.Struct {
			panic(`wrong type of the type, should be a struct`)
		}
		//添加到自定义类型中
		currentTypeName := crtType.Name()
		var structInfo structInfo
		for k := 0; k < crtType.NumField(); k++ {
			crtField := crtType.Field(k)
			crtFieldInfo := getFieldInfo(crtField)
			structInfo.addField(crtFieldInfo)
		}
		gd.entitiesInfos[currentTypeName] = structInfo
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
	if gd.entitiesInfos == nil {
		gd.entitiesInfos = entitiesInfo{}
	}
	gd.entityTableMapping[crtIrf.Name()] = table
	var structInfo structInfo
	fieldCount := reflect.TypeOf(interf).NumField()
	for i := 0; i < fieldCount; i++ {
		currentField := reflect.TypeOf(interf).Field(i)
		if gd.containCustomType(currentField.Type) {
			if customStructConfig, ok := gd.entitiesInfos[currentField.Name]; ok {
				for _, v := range customStructConfig.fieldInfos {
					structInfo.addField(v)
				}
			} else {
				panic(`can't found the required type`)
			}
		} else {
			fieldInfo := getFieldInfo(currentField)
			structInfo.addField(fieldInfo)
		}
	}
	gd.entitiesInfos[crtIrf.Name()] = structInfo
	gd.bondEntities = append(gd.bondEntities, interf)
}

func getFieldInfo(field reflect.StructField) fieldInfo {
	dbTag := field.Tag.Get("db")
	var tableFiled, jsonField string
	var isPrimaryKey, autoFill, isLogicDel bool
	if dbTag == `` {
		tableFiled = strings.ToLower(field.Name)
		if tableFiled == FixedColumnDel {
			isLogicDel = true
		}
	} else if dbTag != `-` {
		tableFiled = dbTag
	}
	jsonTag := field.Tag.Get(`json`)
	if strings.TrimSpace(jsonTag) != `` || strings.TrimSpace(jsonTag) != `-` {
		jsonField = jsonTag
	}
	sqlTag := field.Tag.Get(TagArchType)
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

	return fieldInfo{
		JSONField:    jsonField,
		TableField:   tableFiled,
		Type:         field.Type.Name(),
		IsPrimaryKey: isPrimaryKey,
		AutoFilled:   autoFill,
		Field:        field.Name,
		IsLogicDel:   isLogicDel}
}

func (gd *GenericDao) GetById(intf interface{}, id uint64, force bool, result interface{}) error {
	objType := reflect.TypeOf(intf)
	queryObj := reflect.New(objType)
	queryObj.Elem().FieldByName(`Id`).Set(reflect.ValueOf(null.IntFrom(int64(id))))
	return gd.Get(queryObj.Elem().Interface(), NewDefaultExtraQueryWrapper(), force, result)
}

//GetOne return the pointer of the result object
func (gd *GenericDao) Get(intf interface{}, extraQuery *ExtraQueryWrapper, force bool, result interface{}) error {
	return gd.GetOneWithTx(intf, extraQuery, nil, force, result)
}

func (gd *GenericDao) GetOneWithTx(intf interface{}, extraQuery *ExtraQueryWrapper, tx *sqlx.Tx, force bool, result interface{}) error {
	sqlBuilder := gd.TransferToSelectBuilder(intf, extraQuery)
	sqlQuery, sqlArgs, err := sqlBuilder.ToSql()
	if err != nil {
		return err
	}
	daoExecutor := daoExecutor{gd.db, tx}
	return daoExecutor.get(sqlQuery, sqlArgs, result)
}

func (gd *GenericDao) Select(intf interface{}, result interface{}) error {
	return gd.SelectWithExtraQuery(intf, nil, result)
}

func (gd *GenericDao) SelectWithExtraQuery(queryObj interface{}, extraQuery *ExtraQueryWrapper, result interface{}) error {
	return gd.SelectWithExtraQueryAndTx(queryObj, extraQuery, nil, result)
}

func (gd *GenericDao) SelectWithExtraQueryAndTx(queryObj interface{}, extraQuery *ExtraQueryWrapper, tx *sqlx.Tx, result interface{}) error {
	if extraQuery == nil {
		extraQuery = NewDefaultExtraQueryWrapper()
	}
	if reflect.TypeOf(queryObj).Kind() != reflect.Struct {
		return errors.New(`the interface should be a struct non of pointer`)
	}
	table := gd.entityTableMapping[reflect.TypeOf(queryObj).Name()]
	if table == `` || strings.TrimSpace(table) == `` {
		return errors.New(`no mapping found for the interface` + reflect.TypeOf(queryObj).Name())
	}

	executor := daoExecutor{DB: gd.db, Tx: tx}
	builder := gd.TransferToSelectBuilder(queryObj, extraQuery)
	countSql, sqlArgs, err := sq.Select("count(*)").FromSelect(builder, `t1`).ToSql()
	if err != nil {
		return errors.New(`error occurred when generating the count sql`)
	}
	var totalCount uint64
	if err := executor.get(countSql, sqlArgs, &totalCount); err != nil {
		return err
	}
	extraQuery.Pagination.Total = totalCount
	sqlQuery, sqlArgs, err := sq.Select(`*`).FromSelect(builder, `t1`).
		Offset((extraQuery.Pagination.CurrentPage) * extraQuery.Pagination.PageSize).
		Limit(extraQuery.Pagination.PageSize).ToSql()

	if err != nil {
		return err
	}
	return executor.selectList(sqlQuery, sqlArgs, result)
}

func (gd *GenericDao) SelectList(queryObj interface{}, result interface{}) error {
	return gd.SelectListWithExtraQuery(queryObj, nil, result)
}

func (gd *GenericDao) SelectListWithExtraQuery(queryObj interface{}, extraQuery *ExtraQueryWrapper, result interface{}) error {
	return gd.SelectListWithExtraQueryAndTx(queryObj, extraQuery, nil, result)
}

func (gd *GenericDao) SelectListWithExtraQueryAndTx(queryObj interface{}, extraQuery *ExtraQueryWrapper, tx *sqlx.Tx, result interface{}) error {
	if extraQuery == nil {
		extraQuery = NewDefaultExtraQueryWrapper()
	}
	if reflect.TypeOf(queryObj).Kind() != reflect.Struct {
		return errors.New(`the interface should be a struct non of pointer`)
	}
	table := gd.entityTableMapping[reflect.TypeOf(queryObj).Name()]
	if table == `` || strings.TrimSpace(table) == `` {
		return errors.New(`no mapping found for the interface` + reflect.TypeOf(queryObj).Name())
	}

	executor := daoExecutor{DB: gd.db, Tx: tx}
	builder := gd.TransferToSelectBuilder(queryObj, extraQuery)
	sqlQuery, sqlArgs, err := sq.Select(`*`).FromSelect(builder, `t1`).ToSql()
	if err != nil {
		return err
	}
	return executor.selectList(sqlQuery, sqlArgs, result)
}

func (gd *GenericDao) TransferToSelectBuilder(queryObj interface{}, extraQuery *ExtraQueryWrapper, columns ...string) (selectBuilder sq.SelectBuilder) {
	if extraQuery == nil {
		extraQuery = NewDefaultExtraQueryWrapper()
	}
	if len(columns) <= 0 {
		columns = []string{`*`}
	}
	entityName := reflect.TypeOf(queryObj).Name()
	table := gd.entityTableMapping[entityName]
	var sqlizer, querySqlizer sq.Sqlizer
	var err error

	if querySqlizer, err = extraQuery.Query.ToSQL(); err != nil {
		panic(err)
	}

	if eqClause, _, _ := gd.Validate(queryObj, Select, extraQuery.CurrentUsername); len(eqClause) > 0 {
		andSqlizer := sq.And{}
		andSqlizer = append(andSqlizer, eqClause, querySqlizer)
		sqlizer = andSqlizer
	}

	selectBuilder = sq.Select(columns...).From(table).Where(sqlizer)
	currentColumns := gd.jsonFields2Columns(queryObj, extraQuery.Query.GroupBy)
	selectBuilder = selectBuilder.GroupBy(currentColumns...)
	subOrderBy := make([]string, 0)
	for _, orderByItem := range extraQuery.Query.OrderBy {
		orderByItem.setColumn(gd.jsonFields2Columns(queryObj, orderByItem.JSONFields))
		crtOrderSQL := orderByItem.ToSql()
		if strings.TrimSpace(crtOrderSQL) != `` {
			subOrderBy = append(subOrderBy, crtOrderSQL)
		}
	}
	selectBuilder = selectBuilder.OrderBy(subOrderBy...)
	return selectBuilder
}

func (gd *GenericDao) jsonFields2Columns(queryObj interface{}, jsonFields []string) []string {
	structName := reflect.TypeOf(queryObj).Name()
	columns := make([]string, 0)
	if mappingStruct, exist := gd.entitiesInfos[structName]; exist {
		for _, jsonField := range jsonFields {
			if fieldInfo, exist := mappingStruct.jsonFieldInfos[jsonField]; exist {
				columns = append(columns, fieldInfo.TableField)
			}
		}
	}
	return columns
}

func (gd *GenericDao) Update(queryObj interface{}) (sql.Result, error) {
	return gd.UpdateWithExtraQuery(queryObj, nil)
}

func (gd *GenericDao) UpdateWithExtraQuery(queryObj interface{}, extraQueryWrapper *ExtraQueryWrapper) (sql.Result, error) {
	//tableName := entityTableMapping[reflect.TypeOf(intf).String()]
	return gd.UpdateWithExtraQueryWithTx(queryObj, extraQueryWrapper, nil)
}

//update remove the common fields
func (gd *GenericDao) UpdateWithExtraQueryWithTx(queryObj interface{}, extraQueryWrapper *ExtraQueryWrapper, tx *sqlx.Tx) (sql.Result, error) {
	//tableName := entityTableMapping[reflect.TypeOf(intf).String()]
	if extraQueryWrapper == nil {
		extraQueryWrapper = NewDefaultExtraQueryWrapper()
	}
	if reflect.TypeOf(queryObj).Kind() != reflect.Struct {
		panic(`the interface should be a struct non of pointer`)
	}
	entityName := reflect.TypeOf(queryObj).Name()
	table := gd.entityTableMapping[entityName]

	eqClause, setMap, hasPrimaryKey := gd.Validate(queryObj, Update, extraQueryWrapper.CurrentUsername)
	//fields, values := gd.getValidColumnVal(returnResult, Update, extraQueryWrapper)
	if hasPrimaryKey {
		eqClause = map[string]interface{}{gd.entitiesInfos[entityName].primaryKey.TableField: eqClause[gd.entitiesInfos[entityName].primaryKey.TableField]}
	}
	sqlQuery, args, err := sq.Update(table).SetMap(setMap).Where(eqClause).ToSql()
	if err != nil {
		return nil, err
	}
	executor := daoExecutor{DB: gd.db, Tx: tx}
	return executor.insertOrUpdate(sqlQuery, args)
}

func (gd *GenericDao) Insert(queryObj interface{}) (interface{}, error) {
	return gd.InsertWithExtraQuery(queryObj, nil)
}

func (gd *GenericDao) InsertWithExtraQuery(queryObj interface{}, extraQueryWrapper *ExtraQueryWrapper) (interface{}, error) {
	return gd.InsertWithExtraQueryAndTx(queryObj, extraQueryWrapper, nil)
}

func (gd *GenericDao) InsertWithExtraQueryAndTx(queryObj interface{}, extraQueryWrapper *ExtraQueryWrapper, tx *sqlx.Tx) (interface{}, error) {
	if extraQueryWrapper == nil {
		extraQueryWrapper = NewDefaultExtraQueryWrapper()
	}
	if reflect.TypeOf(queryObj).Kind() != reflect.Struct {
		panic(`the interface should be a struct non of pointer`)
	}
	table, ok := gd.entityTableMapping[reflect.TypeOf(queryObj).Name()]
	if !ok {
		return nil, errors.New(`can't find the configuration for the type of ` + reflect.TypeOf(queryObj).Name())
	}
	_, setMap, _ := gd.Validate(queryObj, Insert, extraQueryWrapper.CurrentUsername)
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
		if _, ok := reflect.TypeOf(queryObj).FieldByName(`Id`); ok {
			result := reflect.New(reflect.TypeOf(queryObj))
			result.Elem().Set(reflect.ValueOf(queryObj))
			err := util.SetFieldValByName(result.Interface(), `Id`, null.IntFrom(insertedId))
			return result.Interface(), err
		}
	}
	return queryObj, err
}

// logical delete
func (gd *GenericDao) Delete(queryObj interface{}) error {
	return gd.DeleteWithExtraQuery(queryObj, nil)
}

func (gd *GenericDao) DeleteWithExtraQuery(queryObj interface{}, extraQueryWrapper *ExtraQueryWrapper) error {
	return gd.DeleteWithExtraQueryAndTx(queryObj, extraQueryWrapper, nil)
}

func (gd *GenericDao) DeleteWithExtraQueryAndTx(queryObj interface{}, extraQueryWrapper *ExtraQueryWrapper, tx *sqlx.Tx) error {
	if reflect.TypeOf(queryObj).Kind() != reflect.Struct {
		return errors.New(`the interface should be a struct non of pointer`)
	}
	if extraQueryWrapper == nil {
		extraQueryWrapper = NewDefaultExtraQueryWrapper()
	}
	entityName := reflect.TypeOf(queryObj).Name()
	table := gd.entityTableMapping[entityName]

	eqClause, setMap, hasPrimaryKey := gd.Validate(queryObj, Delete, extraQueryWrapper.CurrentUsername)
	if hasPrimaryKey {
		eqClause = map[string]interface{}{gd.entitiesInfos[entityName].primaryKey.TableField: eqClause[gd.entitiesInfos[entityName].primaryKey.TableField]}
	}

	var sqlQuery string
	var sqlArgs []interface{}
	var err error
	if gd.entitiesInfos[entityName].LogicDel {
		sqlQuery, sqlArgs, err = sq.Update(table).Where(eqClause).SetMap(setMap).ToSql()
	} else {
		sqlQuery, sqlArgs, err = sq.Delete(table).Where(eqClause).ToSql()
	}
	//sqlQuery, queryArgs, err := updateBuilder.Where(whereSql, values...).ToSql()

	if err != nil {
		return errors.New(`some thing wrong when generating the sql ` + err.Error())
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

func (gd *GenericDao) Validate(queryObj interface{}, operation Operation, executeUser string) (eqClause sq.Eq, setMap map[string]interface{}, primaryKeyValid bool) {
	intfType := reflect.TypeOf(queryObj)
	intfVal := reflect.ValueOf(queryObj)
	//whereClause := sq.Eq{}
	//setClause := make(map[string]interface)
	var returnIntf reflect.Value
	if intfType.Kind() == reflect.Struct {
		returnIntf = reflect.New(intfType)
		returnIntf.Elem().Set(intfVal)
	}
	var structFields structInfo
	var ok bool
	if structFields, ok = gd.entitiesInfos[intfType.Name()]; !ok {
		panic(`can't find the fields configuration for the struct ` + intfType.Name())
	}
	if strings.TrimSpace(executeUser) == `` {
		executeUser = `system`
	}
	eqClause = make(sq.Eq)
	setMap = make(map[string]interface{})
	for i := 0; i < intfType.NumField(); i++ {
		var filedCfg fieldInfo
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
		if filedCfg, ok = structFields.fieldInfos[crtFiledType.Name]; !ok {
			panic(fmt.Sprintf(`can't find the configuration for the struct %s of field %s`, intfType.Name(), crtFiledType.Name))
		}
		if filedCfg.AutoFilled {
			if (strings.ToLower(filedCfg.TableField) == FixedColumnUpdateBy && (operation == Delete || operation == Update)) ||
				(strings.ToLower(filedCfg.TableField) == FixedColumnCreateBy && operation == Insert) {
				setMap[filedCfg.TableField] = null.StringFrom(executeUser)
				continue
			}
			if (strings.ToLower(filedCfg.TableField) == FixedColumnUpdateBy && (operation == Delete || operation == Update)) ||
				(strings.ToLower(filedCfg.TableField) == FixedColumnCreateBy && operation == Insert) {
				setMap[filedCfg.TableField] = util.MyNullTime{Time: null.TimeFrom(time.Now())}
				continue
			}
			if strings.ToLower(filedCfg.TableField) == FixedColumnDel {
				if operation != Insert {
					eqClause[filedCfg.TableField] = null.BoolFrom(false)
				}
				if operation == Delete {
					setMap[filedCfg.TableField] = null.BoolFrom(true)
				}
				continue
			}
		}
		if filedCfg.IsPrimaryKey {
			primaryKeyValid = !crtFiledVal.IsZero()
			if operation != Insert && primaryKeyValid {
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
	//if (operation == Delete || operation == Update) && !primaryKeyValid && !gd.containCustomType(intfType) {
	//	panic(`unsupported query object, should have value for the primary key when execute the update or delete method`)
	//}
	return eqClause, setMap, primaryKeyValid
}
