package go_web_archetype

import (
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/***REMOVED***/go-web-archetype/util"
	"reflect"
	"strings"
)

type SqlTranslate interface {
	ToSQL()(string, []interface{}, error)
}

type QueryItem struct {
	Field    string        `json:"field"`
	Operator QueryOperator `json:"operator"`
	Value    interface{}   `json:"value"`
}

func (qi QueryItem)ToSQL()(sql string, args []interface{}, err error){
	switch qi.Operator {
	case QPIn:
		currentValue := qi.Value
		queryVal := reflect.ValueOf(currentValue)
		if queryVal.Kind() == reflect.String {
			currentValue = strings.Split(qi.Value.(string), `,`)
		}
		inParams := util.InterfaceSlice(currentValue)
		return sq.Eq{qi.Field: inParams}.ToSql()
	case QPEq,QPEqSmb:
		return sq.Eq{qi.Field: qi.Value}.ToSql()
	case QPGt,QPGtSmb:
		return sq.Gt{qi.Field: qi.Value}.ToSql()
	case QPLt,QPLtSmb:
		return sq.Lt{qi.Field: qi.Value}.ToSql()
	case QPGte,QPGteSmb:
		return sq.GtOrEq{qi.Field: qi.Value}.ToSql()
	case QPLike:
		qi.Value = `%` + fmt.Sprint(qi.Value) + `%`
		return sq.Like{qi.Field: qi.Value}.ToSql()
	case QPIs:
		return sq.Eq{qi.Field: qi.Value}.ToSql()
	case QPIsNot:
		return sq.NotEq{qi.Field: qi.Value}.ToSql()
	default:
		return ``, nil, errors.New(`can't find it`)
	}
}


type Connector string

const (
	AND Connector = `AND`
	OR  Connector = `OR`
)

type QueryJSON struct {
	Operator Connector		 `json:"operator"`
	Condition []SqlTranslate `json:"conditions"`
}

func (qj QueryJSON) ToSQL() (sql string, arg []interface{}, err error){
	sql = sql + ` (`
	subQueries := make([]string, 0)
	for _, item := range qj.Condition {
		var crtQuery string
		var crtArgs []interface{}
		switch pred := item.(type) {
		case nil:
			fmt.Println(`test`)
		case QueryItem:
			crtQuery, crtArgs, err = pred.ToSQL()
		case QueryJSON:
			crtQuery, crtArgs, err = pred.ToSQL()
		}
		subQueries = append(subQueries, crtQuery)
		arg = append(arg, crtArgs)
	}
	sql = sql + strings.Join(subQueries,  fmt.Sprintf(` %s `, qj.Condition))
	sql = sql + ` )`
	return sql, arg, err
}


