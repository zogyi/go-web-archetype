package go_web_archetype

import (
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/zogyi/go-web-archetype/base"
	"github.com/zogyi/go-web-archetype/util"
	"reflect"
	"strings"
	"time"
)

type structInfo struct {
	fieldInfos       map[string]base.FieldInfo //所有的字段集合
	jsonFieldInfos   map[string]base.FieldInfo //所有的字段集合
	primaryKey       base.FieldInfo            //主键字段
	autoFilledFields map[string]base.FieldInfo //自动填充字段
	LogicDel         bool
}

func (strFieldInfo *structInfo) GetColumns() (columns []string) {
	for _, info := range strFieldInfo.fieldInfos {
		columns = append(columns, info.TableField)
	}
	return columns
}

func (strFieldInfo *structInfo) IsLogicDelete() bool {
	for _, info := range strFieldInfo.autoFilledFields {
		if info.IsLogicDel {
			return true
		}
	}
	return false
}

func (strFieldInfo *structInfo) IdentifierColumn() (fieldInfo base.FieldInfo, exist bool) {
	for _, info := range strFieldInfo.fieldInfos {
		if info.IsPrimaryKey {
			return info, true
		}
	}
	return
}

func (strFieldInfo *structInfo) addField(fInfo base.FieldInfo) {
	if (base.FieldInfo{}) == fInfo {
		panic(`the param fInfo is null`)
	}
	if strFieldInfo.fieldInfos == nil {
		strFieldInfo.fieldInfos = make(map[string]base.FieldInfo)
	}
	if strFieldInfo.jsonFieldInfos == nil {
		strFieldInfo.jsonFieldInfos = make(map[string]base.FieldInfo)
	}
	if strFieldInfo.autoFilledFields == nil {
		strFieldInfo.autoFilledFields = make(map[string]base.FieldInfo)
	}
	strFieldInfo.fieldInfos[fInfo.Field] = fInfo
	strFieldInfo.jsonFieldInfos[fInfo.JSONField] = fInfo
	if fInfo.IsPrimaryKey {
		if (base.FieldInfo{} != strFieldInfo.primaryKey) && strFieldInfo.primaryKey.TableField != fInfo.TableField {
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

func (gd *DaoQueryHelper) getFieldInfo(structName string, jsonName string) (base.FieldInfo, bool) {
	if structInfo, exist := gd.entitiesInfos[structName]; exist {
		for _, fieldInfo := range structInfo.fieldInfos {
			if fieldInfo.JSONField == jsonName {
				return fieldInfo, true
			}
		}
	}
	return base.FieldInfo{}, false
}

func (gd *DaoQueryHelper) GetColumns(entity string) (columns []string, exist bool) {
	if fieldsInfo, exist := gd.entitiesInfos[entity]; exist {
		return fieldsInfo.GetColumns(), true
	}
	return columns, false
}

func (gd *DaoQueryHelper) GetIdentifier(entity string) (filedInfo base.FieldInfo, exist bool) {
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
			if crtFieldInfo, ok := extractFieldsInfo(crtField); ok {
				structInfo.addField(crtFieldInfo)
			}
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
			if fieldInfo, ok := extractFieldsInfo(currentField); ok {
				structInfo.addField(fieldInfo)
			}
		}
	}
	gd.entitiesInfos[crtIrf.Name()] = structInfo
	gd.bondEntities = append(gd.bondEntities, interf)
}

func extractFieldsInfo(field reflect.StructField) (base.FieldInfo, bool) {
	dbTag := field.Tag.Get("db")
	var tableFiled, jsonField string
	var isPrimaryKey, autoFill, isLogicDel bool
	if dbTag == `` {
		tableFiled = strings.ToLower(field.Name)
	} else if dbTag == `-` {
		return base.FieldInfo{}, false
	} else {
		tableFiled = dbTag
	}

	if tableFiled == base.FixedColumnDel {
		isLogicDel = true
	}

	jsonTag := field.Tag.Get(`json`)
	if strings.TrimSpace(jsonTag) != `` || strings.TrimSpace(jsonTag) != `-` {
		jsonField = jsonTag
	}
	sqlTag := field.Tag.Get(base.TagArchType)
	if strings.TrimSpace(sqlTag) != `` {
		sqlItems := strings.Split(strings.TrimSpace(sqlTag), `,`)
		if util.StringInSlice(base.TagPrimaryKey, sqlItems) {
			isPrimaryKey = true
		}
		if util.StringInSlice(base.TagAutoFilled, sqlItems) {
			autoFill = true
		}
		if !isPrimaryKey && !autoFill {
			panic(fmt.Sprintf(`unrecognized tag value: <%s>, the right format looks like this: "%s:%s,%s"`, sqlTag, base.TagArchType, base.TagPrimaryKey, base.TagAutoFilled))
		}
	}

	return base.FieldInfo{
		JSONField:    jsonField,
		TableField:   tableFiled,
		Type:         field.Type.Name(),
		IsPrimaryKey: isPrimaryKey,
		AutoFilled:   autoFill,
		Field:        field.Name,
		IsLogicDel:   isLogicDel}, true
}

func (gd *DaoQueryHelper) selectPageQuery(queryObj any, extraQuery base.ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	builder := gd.TransferToSelectBuilder(queryObj, extraQuery)
	return sq.Select(`*`).
		FromSelect(builder, `t1`).
		Offset((extraQuery.Pagination.CurrentPage) * extraQuery.Pagination.PageSize).
		Limit(extraQuery.Pagination.PageSize).ToSql()

}

func (gd *DaoQueryHelper) count(queryObj any, extraQuery base.ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	builder := gd.TransferToSelectBuilder(queryObj, extraQuery)
	return sq.Select(`count(*) as totalCount`).FromSelect(builder, `t1`).ToSql()
}

func (gd *DaoQueryHelper) selectQuery(queryObj any, extraQuery base.ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	builder := gd.TransferToSelectBuilder(queryObj, extraQuery)
	return sq.Select(`*`).FromSelect(builder, `t1`).ToSql()
}

func (gd *DaoQueryHelper) TransferToSelectBuilder(queryObj any, extraQuery base.ExtraQueryWrapper, columns ...string) (selectBuilder sq.SelectBuilder) {
	if len(columns) <= 0 {
		columns = []string{`*`}
	}
	entityName := getType(queryObj)
	table := gd.entityTableMapping[entityName]
	entitiesInfos := gd.entitiesInfos[entityName]
	var (
		sqlizer, querySqlizer sq.Sqlizer
		err                   error
		eqClause              = sq.Eq{}
	)

	if !reflect.DeepEqual(extraQuery.QueryExtension.Query, base.Query{}) {
		if querySqlizer, err = extraQuery.QueryExtension.Query.ToSQL(gd.entitiesInfos[entityName].jsonFieldInfos); err != nil {
			panic(err)
		}
	}

	fieldValMap := gd.validate(queryObj)
	for fieldInfo, value := range fieldValMap {
		eqClause[fieldInfo.TableField] = value
	}
	if entitiesInfos.IsLogicDelete() {
		eqClause[base.FixedColumnDel] = false
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

	selectBuilder = sq.Select(columns...).From(table)
	if sqlizer != nil {
		selectBuilder = selectBuilder.Where(sqlizer)
	}
	currentColumns := gd.jsonFields2Columns(queryObj, extraQuery.QueryExtension.GroupBy)
	selectBuilder = selectBuilder.GroupBy(currentColumns...)
	subOrderBy := make([]string, 0)
	for _, orderByItem := range extraQuery.QueryExtension.OrderBy {
		orderByItem.SetColumn(gd.jsonFields2Columns(queryObj, orderByItem.JSONFields))
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
func (gd *DaoQueryHelper) updateQuery(queryObj any, extraQueryWrapper base.ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	entityName := getType(queryObj)
	table := gd.entityTableMapping[entityName]
	entityInfos := gd.entitiesInfos[entityName]
	var (
		querySqlizer sq.Sqlizer
		setMap       = sq.Eq{}
	)

	if !reflect.DeepEqual(extraQueryWrapper.QueryExtension.Query, base.Query{}) {
		if querySqlizer, err = extraQueryWrapper.QueryExtension.Query.ToSQL(gd.entitiesInfos[entityName].jsonFieldInfos); err != nil {
			panic(err)
		}
	}

	fieldValMap := gd.validate(queryObj)
	for fieldInfo, val := range fieldValMap {
		if fieldInfo.AutoFilled {
			if fieldInfo.IsPrimaryKey {
				querySqlizer = sq.Eq{fieldInfo.TableField: val}
			}
			continue
		}
		setMap[fieldInfo.TableField] = val
	}

	if !gd.allowFullTableExecute && querySqlizer == nil {
		err = errors.New(`condition is empty`)
		return
	}

	//TODO: extract this block into a common method
	for _, val := range entityInfos.autoFilledFields {
		if val.TableField == base.FixedColumnUpdateTime {
			setMap[base.FixedColumnUpdateTime] = time.Now()
		} else if val.TableField == base.FixedColumnUpdateBy {
			setMap[base.FixedColumnUpdateBy] = extraQueryWrapper.CurrentUsername
		}
	}
	builder := sq.Update(table).SetMap(setMap)
	if querySqlizer != nil {
		builder = builder.Where(querySqlizer)
	}
	//fields, values := gd.getValidColumnVal(returnResult, Update, extraQueryWrapper)
	return builder.ToSql()
}

func (gd *DaoQueryHelper) insertQuery(queryObj any, extraQueryWrapper base.ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	entityName := getType(queryObj)
	table := gd.entityTableMapping[entityName]
	entityInfos := gd.entitiesInfos[entityName]

	fieldValMap := gd.validate(queryObj)
	setMap := sq.Eq{}

	for fieldInfo, val := range fieldValMap {
		if fieldInfo.AutoFilled {
			err = errors.New(`don't set value for an auto filled field`)
			return
		}
		setMap[fieldInfo.TableField] = val
	}

	//TODO: extract this block into a common method
	for _, val := range entityInfos.autoFilledFields {
		if val.TableField == base.FixedColumnCreateTime {
			setMap[base.FixedColumnCreateTime] = time.Now()
		} else if val.TableField == base.FixedColumnCreateBy {
			setMap[base.FixedColumnCreateBy] = extraQueryWrapper.CurrentUsername
		} else if val.TableField == base.FixedColumnDel {
			setMap[base.FixedColumnDel] = false
		}
	}
	return sq.Insert(table).SetMap(setMap).ToSql()
}

func (gd *DaoQueryHelper) deleteQuery(queryObj any, extraQueryWrapper base.ExtraQueryWrapper) (sql string, args []interface{}, err error) {
	entityName := getType(queryObj)
	table := gd.entityTableMapping[entityName]
	entityInfo := gd.entitiesInfos[entityName]
	var (
		sqlizer, querySqlizer sq.Sqlizer
		eqClause              = sq.Eq{}
	)

	if !reflect.DeepEqual(extraQueryWrapper.QueryExtension.Query, base.Query{}) {
		if querySqlizer, err = extraQueryWrapper.QueryExtension.Query.ToSQL(gd.entitiesInfos[entityName].jsonFieldInfos); err != nil {
			panic(err)
		}
	}

	fieldValMap := gd.validate(queryObj)
	for fieldInfo, val := range fieldValMap {
		if fieldInfo.AutoFilled {
			if fieldInfo.IsPrimaryKey {
				eqClause = sq.Eq{fieldInfo.TableField: val}
				break
			}
		}
		eqClause[fieldInfo.TableField] = val
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
		sqlizer = sq.Eq{}
	}
	if entityInfo.IsLogicDelete() {
		andSqlizer := sq.And{}
		andSqlizer = append(andSqlizer, sqlizer, sq.Eq{base.FixedColumnDel: false})
		sqlizer = andSqlizer
		return sq.Update(table).Set(base.FixedColumnDel, true).Where(sqlizer).ToSql()
	}
	return sq.Delete(table).Where(sqlizer).ToSql()
}

func (gd *DaoQueryHelper) validate(queryObj any) (result map[base.FieldInfo]any) {
	intfType := reflect.TypeOf(queryObj)
	var intfVal, returnIntf reflect.Value
	if intfType.Kind() == reflect.Ptr {
		if reflect.ValueOf(queryObj).IsNil() {
			panic(`invalid param`)
		}
		intfType = intfType.Elem()
		intfVal = reflect.ValueOf(queryObj).Elem()
	} else if intfType.Kind() == reflect.Struct {
		intfVal = reflect.ValueOf(queryObj)
	}

	returnIntf = reflect.New(intfType)
	returnIntf.Elem().Set(intfVal)

	var structFields structInfo
	var ok bool
	if structFields, ok = gd.entitiesInfos[intfType.Name()]; !ok {
		panic(`can't find the fields configuration for the struct ` + intfType.Name())
	}

	result = make(map[base.FieldInfo]any)
	for i := 0; i < intfType.NumField(); i++ {
		var filedCfg base.FieldInfo
		crtFiledType := intfType.Field(i)
		crtFiledVal := returnIntf.Elem().FieldByName(crtFiledType.Name)
		if gd.containCustomType(crtFiledType.Type) {
			subResult := gd.validate(crtFiledVal.Interface())
			for k, v := range subResult {
				result[k] = v
			}
			continue
		}
		dbTag := crtFiledType.Tag.Get(`db`)
		if strings.TrimSpace(dbTag) == `-` || strings.TrimSpace(dbTag) == `` {
			continue
		}
		if filedCfg, ok = structFields.fieldInfos[crtFiledType.Name]; !ok {
			panic(fmt.Sprintf(`can't find the configuration for the struct %s of field %s`, intfType.Name(), crtFiledType.Name))
		}
		if !crtFiledVal.IsZero() {
			result[filedCfg] = crtFiledVal.Interface()
		}
	}
	return
}

func getType(queryObj any) string {
	if t := reflect.TypeOf(queryObj); t.Kind() == reflect.Ptr {
		if reflect.ValueOf(queryObj).IsNil() {
			panic(`invalid param`)
		}
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}
