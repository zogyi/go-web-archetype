package go_web_archetype

import (
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/zogyi/go-web-archetype/util"
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
	Pagination      util.Pagination
	QueryExtension  QueryExtension
}

const (
	FixedColumnCreateBy   string = `create_by`
	FixedColumnUpdateBy   string = `update_by`
	FixedColumnCreateTime string = `create_time`
	FixedColumnUpdateTime string = `update_time`
	FixedColumnDel        string = `del`
	TagArchType           string = `archType`
	TagPrimaryKey         string = `primaryKey`
	TagAutoFilled         string = `autoFill`
)

type FieldInfo struct {
	Field        string
	JSONField    string
	TableField   string
	Type         string
	IsLogicDel   bool
	IsPrimaryKey bool
	AutoFilled   bool
}

type structInfo struct {
	fieldInfos       map[string]FieldInfo //所有的字段集合
	jsonFieldInfos   map[string]FieldInfo //所有的字段集合
	primaryKey       FieldInfo            //主键字段
	autoFilledFields map[string]FieldInfo //自动填充字段
	LogicDel         bool
}

func (strFieldInfo *structInfo) GetColumns() (columns []string) {
	for _, info := range strFieldInfo.fieldInfos {
		columns = append(columns, info.TableField)
	}
	return columns
}

func (strFieldInfo *structInfo) IdentifierColumn() (fieldInfo FieldInfo, exist bool) {
	for _, info := range strFieldInfo.fieldInfos {
		if info.IsPrimaryKey {
			return info, true
		}
	}
	return
}

func (strFieldInfo *structInfo) addField(fInfo FieldInfo) {
	if (FieldInfo{}) == fInfo {
		panic(`the param fInfo is null`)
	}
	if strFieldInfo.fieldInfos == nil {
		strFieldInfo.fieldInfos = make(map[string]FieldInfo)
	}
	if strFieldInfo.jsonFieldInfos == nil {
		strFieldInfo.jsonFieldInfos = make(map[string]FieldInfo)
	}
	if strFieldInfo.autoFilledFields == nil {
		strFieldInfo.autoFilledFields = make(map[string]FieldInfo)
	}
	strFieldInfo.fieldInfos[fInfo.Field] = fInfo
	strFieldInfo.jsonFieldInfos[fInfo.JSONField] = fInfo
	if fInfo.IsPrimaryKey {
		if (FieldInfo{} != strFieldInfo.primaryKey) && strFieldInfo.primaryKey.TableField != fInfo.TableField {
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

type DaoQueryHelper struct {
	bondEntities       []interface{}     //所有的entity
	entityTableMapping map[string]string //entity与table名字之间的映射
	entitiesInfos      entitiesInfo      //entity field所有的表单的映射
	//tablePrimaryKey    map[string]*fieldInfo       //table's primary key mapping
	//自定义类型如下
	allowFullTableExecute bool
	customType            []interface{} //用户自定义的类型
	//customTypeFieldMapping map[string]map[string]*fieldInfo //自定义类型的字段
	//commonFields 	   util.CommonFields
}

func NewDaoQueryHelper(types ...interface{}) *DaoQueryHelper {
	queryHelper := DaoQueryHelper{}
	queryHelper.AddCustomType(types...)
	return &queryHelper
}

func (gd *DaoQueryHelper) setFullTableExecute(fullExecute bool) {
	gd.allowFullTableExecute = fullExecute
}

func (gd *DaoQueryHelper) getFieldInfo(structName string, jsonName string) (FieldInfo, bool) {
	if structInfo, exist := gd.entitiesInfos[structName]; exist {
		for _, fieldInfo := range structInfo.fieldInfos {
			if fieldInfo.JSONField == jsonName {
				return fieldInfo, true
			}
		}
	}
	return FieldInfo{}, false
}

func (gd *DaoQueryHelper) GetColumns(entity string) (columns []string, exist bool) {
	if fieldsInfo, exist := gd.entitiesInfos[entity]; exist {
		return fieldsInfo.GetColumns(), true
	}
	return columns, false
}

func (gd *DaoQueryHelper) GetIdentifier(entity string) (filedInfo FieldInfo, exist bool) {
	if structInfo, exist := gd.entitiesInfos[entity]; exist {
		return structInfo.IdentifierColumn()
	}
	return
}

func (gd *DaoQueryHelper) GetEntityTable(queryObj any) (string, bool) {
	return gd.GetTable(reflect.TypeOf(queryObj).Name())
}

func (gd *DaoQueryHelper) GetTable(entity string) (string, bool) {
	if table, exist := gd.entityTableMapping[entity]; exist {
		return table, true
	}
	return ``, false
}

func (gd *DaoQueryHelper) AddCustomType(types ...interface{}) *DaoQueryHelper {
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

func (gd *DaoQueryHelper) containCustomType(fieldType reflect.Type) bool {
	for i := 0; i < len(gd.customType); i++ {
		if reflect.TypeOf(gd.customType[i]) == fieldType {
			return true
		}
	}
	return false
}

func (gd *DaoQueryHelper) GetBondEntities() []interface{} {
	return gd.bondEntities
}

func (gd *DaoQueryHelper) GetEntityTableMapping() map[string]string {
	return gd.entityTableMapping
}

func (gd *DaoQueryHelper) Bind(interf interface{}, table string) {
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

func getFieldInfo(field reflect.StructField) FieldInfo {
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

	return FieldInfo{
		JSONField:    jsonField,
		TableField:   tableFiled,
		Type:         field.Type.Name(),
		IsPrimaryKey: isPrimaryKey,
		AutoFilled:   autoFill,
		Field:        field.Name,
		IsLogicDel:   isLogicDel}
}

func (gd *DaoQueryHelper) selectPageQuery(queryObj any, extraQuery ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	builder := gd.TransferToSelectBuilder(queryObj, extraQuery)
	return sq.Select(`*`).
		FromSelect(builder, `t1`).
		Offset((extraQuery.Pagination.CurrentPage) * extraQuery.Pagination.PageSize).
		Limit(extraQuery.Pagination.PageSize).ToSql()

}

func (gd *DaoQueryHelper) count(queryObj any, extraQuery ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	builder := gd.TransferToSelectBuilder(queryObj, extraQuery)
	return sq.Select(`count(*) as totalCount`).FromSelect(builder, `t1`).ToSql()
}

func (gd *DaoQueryHelper) selectQuery(queryObj any, extraQuery ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	builder := gd.TransferToSelectBuilder(queryObj, extraQuery)
	return sq.Select(`*`).FromSelect(builder, `t1`).ToSql()
}

func (gd *DaoQueryHelper) TransferToSelectBuilder(queryObj any, extraQuery ExtraQueryWrapper, columns ...string) (selectBuilder sq.SelectBuilder) {
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

	eqClause, _, _ = gd.validate(queryObj, Select, extraQuery.CurrentUsername)
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

func (gd *DaoQueryHelper) jsonFields2Columns(queryObj any, jsonFields []string) []string {
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

//update remove the common fields
func (gd *DaoQueryHelper) updateQuery(queryObj any, extraQueryWrapper ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	entityName := reflect.TypeOf(queryObj).Name()
	table := gd.entityTableMapping[entityName]
	var (
		querySqlizer  sq.Sqlizer
		fieldVal      sq.Eq
		setMap        map[string]interface{}
		hasPrimaryKey bool
	)

	if !reflect.DeepEqual(extraQueryWrapper.QueryExtension.Query, Query{}) {
		if querySqlizer, err = extraQueryWrapper.QueryExtension.Query.ToSQL(gd.entitiesInfos[entityName].jsonFieldInfos); err != nil {
			panic(err)
		}
	}

	fieldVal, setMap, hasPrimaryKey = gd.validate(queryObj, Update, extraQueryWrapper.CurrentUsername)
	if hasPrimaryKey {
		eqClause := map[string]interface{}{gd.entitiesInfos[entityName].primaryKey.TableField: fieldVal[gd.entitiesInfos[entityName].primaryKey.TableField]}
		return sq.Update(table).SetMap(setMap).Where(eqClause).ToSql()
	}

	if !gd.allowFullTableExecute && querySqlizer == nil {
		err = errors.New(`condition is empty`)
		return
	}
	builder := sq.Update(table).SetMap(setMap)
	if querySqlizer != nil {
		builder = builder.Where(querySqlizer)
	}
	//fields, values := gd.getValidColumnVal(returnResult, Update, extraQueryWrapper)
	return builder.ToSql()
}

func (gd *DaoQueryHelper) insertQuery(queryObj any, extraQueryWrapper ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	entityName := reflect.TypeOf(queryObj).Name()
	table := gd.entityTableMapping[entityName]
	_, setMap, _ := gd.validate(queryObj, Insert, extraQueryWrapper.CurrentUsername)
	return sq.Insert(table).SetMap(setMap).ToSql()
}

func (gd *DaoQueryHelper) deleteQuery(queryObj any, extraQueryWrapper ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	entityName := reflect.TypeOf(queryObj).Name()
	table := gd.entityTableMapping[entityName]
	var (
		sqlizer, querySqlizer sq.Sqlizer
		eqClause              sq.Eq
		hasPrimaryKey         bool
	)

	if !reflect.DeepEqual(extraQueryWrapper.QueryExtension.Query, Query{}) {
		if querySqlizer, err = extraQueryWrapper.QueryExtension.Query.ToSQL(gd.entitiesInfos[entityName].jsonFieldInfos); err != nil {
			panic(err)
		}
	}

	eqClause, _, hasPrimaryKey = gd.validate(queryObj, Delete, extraQueryWrapper.CurrentUsername)
	if hasPrimaryKey {
		eqClause := map[string]interface{}{gd.entitiesInfos[entityName].primaryKey.TableField: eqClause[gd.entitiesInfos[entityName].primaryKey.TableField]}
		return sq.Delete(table).Where(eqClause).ToSql()
	}
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
		if !gd.allowFullTableExecute {
			err = errors.New(`condition is empty`)
			return
		}
		return sq.Delete(table).ToSql()
	}
	return sq.Delete(table).Where(sqlizer).ToSql()
}

func (gd *DaoQueryHelper) validate(queryObj any, operation Operation, executeUser string) (eqClause sq.Eq, setMap map[string]interface{}, primaryKeyValid bool) {
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
		var filedCfg FieldInfo
		crtFiledType := intfType.Field(i)
		crtFiledVal := returnIntf.Elem().FieldByName(crtFiledType.Name)
		if gd.containCustomType(crtFiledType.Type) {
			subEqClause, subSetMap, subPrimaryKeyValid := gd.validate(crtFiledVal.Interface(), operation, executeUser)
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
				setMap[filedCfg.TableField] = executeUser
			}
			if (strings.ToLower(filedCfg.TableField) == FixedColumnUpdateTime && (operation == Delete || operation == Update)) ||
				(strings.ToLower(filedCfg.TableField) == FixedColumnCreateTime && operation == Insert) {
				setMap[filedCfg.TableField] = time.Now()
			}
			if strings.ToLower(filedCfg.TableField) == FixedColumnDel {
				if operation != Insert {
					eqClause[filedCfg.TableField] = false
				}
				if operation == Delete {
					setMap[filedCfg.TableField] = true
				}
			}
			continue
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
	return eqClause, setMap, primaryKeyValid
}
