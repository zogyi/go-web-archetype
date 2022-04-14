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

type QueryExtension struct {
	Query   Query     `json:"query"`
	GroupBy []string  `json:"groupBy"`
	OrderBy []OrderBy `json:"orderBy"`
}

type ExtraQueryWrapper struct {
	CurrentUsername string
	Pagination      *util.Pagination
	QueryExtension  *QueryExtension
}

func NewDefaultExtraQueryWrapper() *ExtraQueryWrapper {
	return &ExtraQueryWrapper{Pagination: &util.Pagination{PageSize: 10, CurrentPage: 0}, QueryExtension: &QueryExtension{}}
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

type GenericDao[T Connection] struct {
	db                 T
	bondEntities       []interface{}     //所有的entity
	entityTableMapping map[string]string //entity与table名字之间的映射
	entitiesInfos      entitiesInfo      //entity field所有的表单的映射
	//tablePrimaryKey    map[string]*fieldInfo       //table's primary key mapping
	//自定义类型如下
	customType []interface{} //用户自定义的类型
	//customTypeFieldMapping map[string]map[string]*fieldInfo //自定义类型的字段
	//commonFields 	   util.CommonFields
}

func (gd *GenericDao[T]) getFieldInfo(structName string, jsonName string) (fieldInfo, bool) {
	if structInfo, exist := gd.entitiesInfos[structName]; exist {
		for _, fieldInfo := range structInfo.fieldInfos {
			if fieldInfo.JSONField == jsonName {
				return fieldInfo, true
			}
		}
	}
	return fieldInfo{}, false
}

func (gd *GenericDao[T]) GetColumns(entity string) (columns []string, exist bool) {
	if fieldsInfo, exist := gd.entitiesInfos[entity]; exist {
		return fieldsInfo.GetColumns(), true
	}
	return columns, false
}

func (gd *GenericDao[T]) GetTable(entity string) (string, bool) {
	if table, exist := gd.entityTableMapping[entity]; exist {
		return table, true
	}
	return ``, false
}

func NewGenericDao(db *sqlx.DB) *GenericDao[*sqlx.DB] {
	if db == nil {
		panic(`the pointer of database is nil`)
	}
	if err := db.Ping(); err != nil {
		zap.L().Error(`can't connect to the database`)
		panic(`can't connect to the database`)
	}
	return &GenericDao[*sqlx.DB]{db: db}
}

func NewDaoWithDefaultCustomerType(db *sqlx.DB) *GenericDao[*sqlx.DB] {
	dao := NewGenericDaoWithCustomerType(db, CommonFields{}, CommonDel{})
	return dao
}

func NewGenericDaoWithCustomerType(db *sqlx.DB, types ...interface{}) *GenericDao[*sqlx.DB] {
	dao := NewGenericDao(db)
	dao.AddCustomType(types...)
	return dao
}

func (gd *GenericDao[T]) AddCustomType(types ...interface{}) *GenericDao[T] {
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

func (gd *GenericDao[T]) containCustomType(fieldType reflect.Type) bool {
	for i := 0; i < len(gd.customType); i++ {
		if reflect.TypeOf(gd.customType[i]) == fieldType {
			return true
		}
	}
	return false
}

func (gd *GenericDao[T]) DB() T {
	return gd.db
}

func (gd *GenericDao[T]) GetBondEntities() []interface{} {
	return gd.bondEntities
}

func (gd *GenericDao[T]) GetEntityTableMapping() map[string]string {
	return gd.entityTableMapping
}

func BeginTx(dao *GenericDao[*sqlx.DB]) (*GenericDao[*sqlx.Tx], error) {
	if tx, err := dao.DB().Beginx(); err == nil {
		return &GenericDao[*sqlx.Tx]{
			db:                 tx,
			bondEntities:       dao.bondEntities,
			entityTableMapping: dao.entityTableMapping,
			entitiesInfos:      dao.entitiesInfos,
			customType:         dao.customType,
		}, nil
	} else {
		return nil, err
	}
}

func (gd *GenericDao[T]) Bind(interf interface{}, table string) {
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
	} else {
		tableFiled = dbTag
	}

	if tableFiled == FixedColumnDel {
		isLogicDel = true
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

func (gd *GenericDao[T]) GetById(intf interface{}, id uint64, force bool, result interface{}) error {
	objType := reflect.TypeOf(intf)
	queryObj := reflect.New(objType)
	queryObj.Elem().FieldByName(`Id`).Set(reflect.ValueOf(null.IntFrom(int64(id))))
	return gd.Get(queryObj.Elem().Interface(), NewDefaultExtraQueryWrapper(), force, result)
}

func (gd *GenericDao[T]) GetOne(intf interface{}, result interface{}) error {
	return gd.Get(intf, nil, false, result)
}

func (gd *GenericDao[T]) Get(intf interface{}, extraQuery *ExtraQueryWrapper, force bool, result interface{}) error {
	sqlBuilder := gd.TransferToSelectBuilder(intf, extraQuery)
	sqlQuery, sqlArgs, err := sqlBuilder.ToSql()
	if err != nil {
		return err
	}
	return get(gd.db, sqlQuery, sqlArgs, result)
}

func (gd *GenericDao[T]) Select(intf interface{}, result interface{}) error {
	return gd.SelectWithExtraQuery(intf, NewDefaultExtraQueryWrapper(), result)
}

func (gd *GenericDao[T]) SelectWithExtraQuery(queryObj interface{}, extraQuery *ExtraQueryWrapper, result interface{}) error {
	if reflect.TypeOf(queryObj).Kind() != reflect.Struct {
		return errors.New(`the interface should be a struct non of pointer`)
	}
	table := gd.entityTableMapping[reflect.TypeOf(queryObj).Name()]
	if table == `` || strings.TrimSpace(table) == `` {
		return errors.New(`no mapping found for the interface` + reflect.TypeOf(queryObj).Name())
	}

	builder := gd.TransferToSelectBuilder(queryObj, extraQuery)
	countSql, sqlArgs, err := sq.Select("count(*)").FromSelect(builder, `t1`).ToSql()
	if err != nil {
		return errors.New(`error occurred when generating the count sql`)
	}
	var totalCount uint64
	if err := get(gd.DB(), countSql, sqlArgs, &totalCount); err != nil {
		return err
	}
	extraQuery.Pagination.Total = totalCount
	sqlQuery, sqlArgs, err := sq.Select(`*`).FromSelect(builder, `t1`).
		Offset((extraQuery.Pagination.CurrentPage) * extraQuery.Pagination.PageSize).
		Limit(extraQuery.Pagination.PageSize).ToSql()

	if err != nil {
		return err
	}
	return selectList(gd.DB(), sqlQuery, sqlArgs, result)
}

func (gd *GenericDao[T]) SelectList(queryObj interface{}, result interface{}) error {
	return gd.SelectListWithExtraQuery(queryObj, NewDefaultExtraQueryWrapper(), result)
}

func (gd *GenericDao[T]) SelectListWithExtraQuery(queryObj interface{}, extraQuery *ExtraQueryWrapper, result interface{}) error {
	return gd.SelectListWithExtraQueryAndTx(queryObj, extraQuery, nil, result)
}

func (gd *GenericDao[T]) SelectListWithExtraQueryAndTx(queryObj interface{}, extraQuery *ExtraQueryWrapper, tx *sqlx.Tx, result interface{}) error {
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

	builder := gd.TransferToSelectBuilder(queryObj, extraQuery)
	sqlQuery, sqlArgs, err := sq.Select(`*`).FromSelect(builder, `t1`).ToSql()
	if err != nil {
		return err
	}
	return selectList(gd.db, sqlQuery, sqlArgs, result)
}

func (gd *GenericDao[T]) TransferToSelectBuilder(queryObj interface{}, extraQuery *ExtraQueryWrapper, columns ...string) (selectBuilder sq.SelectBuilder) {
	if extraQuery == nil {
		extraQuery = NewDefaultExtraQueryWrapper()
	}
	if len(columns) <= 0 {
		columns = []string{`*`}
	}
	entityName := reflect.TypeOf(queryObj).Name()
	table := gd.entityTableMapping[entityName]
	var (
		sqlizer, querySqlizer sq.Sqlizer
		err                   error
		eqClause              sq.Eq
	)

	if !reflect.DeepEqual(extraQuery.QueryExtension.Query, Query{}) {
		if querySqlizer, err = extraQuery.QueryExtension.Query.ToSQL(gd.entitiesInfos[entityName].jsonFieldInfos); err != nil {
			panic(err)
		}
	}

	eqClause, _, _ = gd.Validate(queryObj, Select, extraQuery.CurrentUsername)
	if querySqlizer != nil && eqClause != nil && len(eqClause) > 0 {
		andSqlizer := sq.And{}
		andSqlizer = append(andSqlizer, eqClause, querySqlizer)
		sqlizer = andSqlizer
	} else if querySqlizer == nil && eqClause != nil && len(eqClause) > 0 {
		sqlizer = eqClause
	} else if querySqlizer != nil && (eqClause == nil || len(eqClause) == 0) {
		sqlizer = querySqlizer
	}

	selectBuilder = sq.Select(columns...).From(table)
	if sqlizer != nil {
		selectBuilder = selectBuilder.Where(sqlizer)
	}
	currentColumns := gd.jsonFields2Columns(queryObj, extraQuery.QueryExtension.GroupBy)
	selectBuilder = selectBuilder.GroupBy(currentColumns...)
	subOrderBy := make([]string, 0)
	for _, orderByItem := range extraQuery.QueryExtension.OrderBy {
		orderByItem.setColumn(gd.jsonFields2Columns(queryObj, orderByItem.JSONFields))
		crtOrderSQL := orderByItem.ToSql()
		if strings.TrimSpace(crtOrderSQL) != `` {
			subOrderBy = append(subOrderBy, crtOrderSQL)
		}
	}
	selectBuilder = selectBuilder.OrderBy(subOrderBy...)
	return selectBuilder
}

func (gd *GenericDao[T]) jsonFields2Columns(queryObj interface{}, jsonFields []string) []string {
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

func (gd *GenericDao[T]) Update(queryObj interface{}) (sql.Result, error) {
	return gd.UpdateWithExtraQuery(queryObj, NewDefaultExtraQueryWrapper())
}

//update remove the common fields
func (gd *GenericDao[T]) UpdateWithExtraQuery(queryObj interface{}, extraQueryWrapper *ExtraQueryWrapper) (sql.Result, error) {
	//tableName := entityTableMapping[reflect.TypeOf(intf).String()]
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
	return execute(gd.DB(), sqlQuery, args)
}

func (gd *GenericDao[T]) Insert(queryObj interface{}) (interface{}, error) {
	return gd.InsertWithExtraQuery(queryObj, NewDefaultExtraQueryWrapper())
}

func (gd *GenericDao[T]) InsertWithExtraQuery(queryObj interface{}, extraQueryWrapper *ExtraQueryWrapper) (interface{}, error) {
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
	result, err := execute(gd.DB(), sqlQuery, sqlArgs)

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
func (gd *GenericDao[T]) Delete(queryObj interface{}) error {
	return gd.DeleteWithExtraQuery(queryObj, NewDefaultExtraQueryWrapper())
}

func (gd *GenericDao[T]) DeleteWithExtraQuery(queryObj interface{}, extraQueryWrapper *ExtraQueryWrapper) error {
	if reflect.TypeOf(queryObj).Kind() != reflect.Struct {
		return errors.New(`the interface should be a struct non of pointer`)
	}
	entityName := reflect.TypeOf(queryObj).Name()
	table := gd.entityTableMapping[entityName]

	var (
		sqlizer, querySqlizer sq.Sqlizer
		err                   error
		setMap                map[string]interface{}
		eqClause              sq.Eq
	)

	if extraQueryWrapper.QueryExtension != nil && !reflect.DeepEqual(extraQueryWrapper.QueryExtension.Query, Query{}) {
		if querySqlizer, err = extraQueryWrapper.QueryExtension.Query.ToSQL(gd.entitiesInfos[entityName].jsonFieldInfos); err != nil {
			panic(err)
		}
	}

	eqClause, setMap, _ = gd.Validate(queryObj, Delete, extraQueryWrapper.CurrentUsername)
	if querySqlizer != nil && eqClause != nil && len(eqClause) > 0 {
		andSqlizer := sq.And{}
		andSqlizer = append(andSqlizer, eqClause, querySqlizer)
		sqlizer = andSqlizer
	} else if querySqlizer == nil && eqClause != nil && len(eqClause) > 0 {
		sqlizer = eqClause
	} else if querySqlizer != nil && (eqClause == nil || len(eqClause) == 0) {
		sqlizer = querySqlizer
	}
	if querySqlizer == nil && (eqClause == nil || len(eqClause) == 0) {
		return errors.New(`condition is empty`)
	}

	var sqlQuery string
	var sqlArgs []interface{}
	if gd.entitiesInfos[entityName].LogicDel {
		sqlQuery, sqlArgs, err = sq.Update(table).Where(sqlizer).SetMap(setMap).ToSql()
	} else {
		sqlQuery, sqlArgs, err = sq.Delete(table).Where(sqlizer).ToSql()
	}
	//sqlQuery, queryArgs, err := updateBuilder.Where(whereSql, values...).ToSql()

	if err != nil {
		return errors.New(`some thing wrong when generating the sql ` + err.Error())
	}
	var sqlResult sql.Result
	var rows int64
	if sqlResult, err = execute(gd.DB(), sqlQuery, sqlArgs); err == nil {
		if rows, err = sqlResult.RowsAffected(); err == nil && rows <= 0 {
			return errors.New(`no rows effected`)
		}
	}
	return err
}

func (gd *GenericDao[T]) Validate(queryObj interface{}, operation Operation, executeUser string) (eqClause sq.Eq, setMap map[string]interface{}, primaryKeyValid bool) {
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
