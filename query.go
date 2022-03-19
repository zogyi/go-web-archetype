package go_web_archetype

import (
	"encoding/json"
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/***REMOVED***/go-web-archetype/util"
	"reflect"
	"strings"
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
	QPIs     QueryOperator = `is`
	QPIsNot  QueryOperator = `is not`
	QPIn     QueryOperator = `in`
)

type SqlTranslate interface {
	ToSQL(map[string]fieldInfo) (sq.Sqlizer, error)
}

type QueryItem struct {
	Field    string        `json:"field"` //JSON field
	Operator QueryOperator `json:"operator"`
	Value    interface{}   `json:"value"`
}

func (qi *QueryItem) UnmarshalJSON(data []byte) (err error) {
	var queryItemRawMsg map[string]*json.RawMessage
	if err = json.Unmarshal(data, &queryItemRawMsg); err == nil {
		for key, val := range queryItemRawMsg {
			switch key {
			case `field`:
				if err = json.Unmarshal(*val, &qi.Field); err != nil {
					return err
				}
				break
			case `operator`:
				if err = json.Unmarshal(*val, &qi.Operator); err != nil {
					return err
				}
				break
			case `value`:
				if err = json.Unmarshal(*val, &qi.Value); err != nil {
					return err
				}
				break
			default:
				return errors.New(`unmatched format`)
			}
		}
	}
	return
}

func (qi QueryItem) ToSQL(json2Fields map[string]fieldInfo) (sqlizer sq.Sqlizer, err error) {
	var column string
	if fieldInfo, exist := json2Fields[qi.Field]; exist{
		column = fieldInfo.TableField
	} else {
		return nil, errors.New(`can't find the column`)
	}
	switch qi.Operator {
	case QPIn:
		currentValue := qi.Value
		queryVal := reflect.ValueOf(currentValue)
		if queryVal.Kind() == reflect.String {
			currentValue = strings.Split(qi.Value.(string), `,`)
		}
		inParams := util.InterfaceSlice(currentValue)
		return sq.Eq{column: inParams}, nil
	case QPEq, QPEqSmb:
		return sq.Eq{column: qi.Value}, nil
	case QPGt, QPGtSmb:
		return sq.Gt{column: qi.Value}, nil
	case QPLt, QPLtSmb:
		return sq.Lt{column: qi.Value}, nil
	case QPGte, QPGteSmb:
		return sq.GtOrEq{column: qi.Value}, nil
	case QPLte, QPLteSmb:
		return sq.LtOrEq{column: qi.Value}, nil
	case QPLike:
		qi.Value = `%` + fmt.Sprint(qi.Value) + `%`
		return sq.Like{column: qi.Value}, nil
	case QPIs:
		return sq.Eq{column: qi.Value}, nil
	case QPIsNot:
		return sq.NotEq{column: qi.Value}, nil
	default:
		return nil, errors.New(`can't find it`)
	}
}

type Connector string

func (c *Connector) UnmarshalJSON(data []byte) (err error) {
	var currentStr string
	if err := json.Unmarshal(data, &currentStr); err != nil {
		return err
	}
	if strings.ToUpper(currentStr) == string(AND) {
		*c = AND
	} else if strings.ToUpper(currentStr) == string(OR) {
		*c = OR
	} else {
		err = errors.New(`connector type not found`)
	}
	return err
}

const (
	AND Connector = `AND`
	OR  Connector = `OR`
)

type Query struct {
	Operator  Connector      `json:"connector"`
	Condition []SqlTranslate `json:"conditions"`
}

func (m *Query) UnmarshalJSON(data []byte) (err error) {
	var queryJSONRawMsg map[string]*json.RawMessage
	if err := json.Unmarshal(data, &queryJSONRawMsg); err != nil {
		return err
	}
	for key, val := range queryJSONRawMsg {
		switch key {
		case `connector`:
			var connector Connector
			if err := json.Unmarshal(*val, &connector); err != nil {
				return err
			}
			m.Operator = connector
		case `conditions`:
			conditionsRawData := make([]*json.RawMessage, 0)
			m.Condition = make([]SqlTranslate, 0)
			if err := json.Unmarshal(*val, &conditionsRawData); err != nil {
				return err
			}
			for _, item := range conditionsRawData {
				var queryItem QueryItem
				if err := json.Unmarshal(*item, &queryItem); err != nil {
					var queryJSON Query
					if err = json.Unmarshal(*item, &queryJSON); err != nil {
						return err
					} else {
						m.Condition = append(m.Condition, queryJSON)
					}
				} else {
					m.Condition = append(m.Condition, queryItem)
				}
			}
		default:
			return errors.New(`type not match`)
		}
	}
	return
}

//func (qi QueryItem)ToSQL()(sqlizer sq.Sqlizer, err error)
func (qj Query) ToSQL(json2Fields map[string]fieldInfo) (sqlizer sq.Sqlizer, err error) {
	if qj.Condition == nil || len(qj.Condition) == 0 {
		return nil, errors.New(`condition array is empty`)
	} else if len(qj.Condition) == 1 {
		return qj.Condition[0].ToSQL(json2Fields)
	}
	operatorAnd := sq.And{}
	operatorOr := sq.Or{}
	for _, item := range qj.Condition {
		var err error
		var curentSqlizer sq.Sqlizer
		switch pred := item.(type) {
		case nil:
			err = errors.New(`not supported nil format`)
		case QueryItem:
			curentSqlizer, err = pred.ToSQL(json2Fields)
		case Query:
			curentSqlizer, err = pred.ToSQL(json2Fields)
		default:
			errors.New(`not supported type`)
		}
		if err != nil {
			return nil, err
		}
		if qj.Operator == AND {
			operatorAnd = append(operatorAnd, curentSqlizer)
		} else if qj.Operator == OR {
			operatorOr = append(operatorOr, curentSqlizer)
		} else {
			return nil, errors.New(`not supported connector`)
		}
	}
	if qj.Operator == AND {
		return operatorAnd, nil
	}
	return operatorOr, nil
}
