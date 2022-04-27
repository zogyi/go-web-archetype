package util

import (
	"context"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"gopkg.in/guregu/null.v3"
	"reflect"
	"regexp"
	"strings"
	"time"
)

type txContext struct {
}

func SetTxContext(ctx context.Context, tx *sqlx.Tx) context.Context {
	return context.WithValue(ctx, txContext{}, tx)
}

func ExtractTx(ctx context.Context) (tx *sqlx.Tx, ok bool) {
	if ctx.Value(txContext{}) != nil {
		if tx, ok = ctx.Value(txContext{}).(*sqlx.Tx); ok {
			return
		}
	}
	return
}

type Pagination struct {
	PageSize    uint64 `form:"pageSize"`
	CurrentPage uint64 `form:"currentPage"`
	Total       uint64
}

type RolePath struct {
	Role string   `yaml:"role"`
	Path []string `yaml:"path"`
}

type FieldsMapping map[string]map[string]string

type PaginationResult struct {
	List  interface{} `json:"list"`
	Total uint64      `json:"total"`
}

type ResponseObj struct {
	Success bool        `json:"success"`
	ErrCode int         `json:"errCode"`
	ErrMsg  string      `json:"errMsg"`
	Result  interface{} `json:"result"`
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

type MyNullTime struct {
	null.Time
}

func (myNullTime MyNullTime) MarshalJSON() ([]byte, error) {
	result, err := myNullTime.Time.MarshalJSON()
	if err == nil {
		resultStr := string(result)
		resultStr = strings.Replace(resultStr, "T", " ", 1)
		resultStr = strings.Replace(resultStr, "Z", "", 1)
		result = []byte(resultStr)
	}
	return result, err
}

func (myNullTime *MyNullTime) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), "\"")
	if s == `null` {
		myNullTime = &MyNullTime{Time: null.Time{Valid: false}}
		return nil
	}
	t, err := time.Parse(`2006-01-02`, s)
	if err != nil {
		t, err = time.Parse(`2006-01-02 15:04:05`, s)
		if err != nil {
			t, err = time.Parse(`2006-01-02T15:04:05.000Z`, s)
			if err != nil {
				return errors.New(`not supported date format ` + s)
			}
		}
	}
	myNullTime.Time = null.TimeFrom(t)
	return nil
}

func CreateObjFromInterface(interf interface{}) reflect.Value {
	currentType := reflect.TypeOf(interf)
	if currentType.Kind() != reflect.Struct {
		panic(`the interface should be an struct non pointer`)
	}
	return reflect.New(currentType)
}

func InterfaceSlice(slice interface{}) []interface{} {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		panic("InterfaceSlice() given a non-slice type")
	}

	// Keep the distinction between nil and empty slice input
	if s.IsNil() {
		return nil
	}

	ret := make([]interface{}, s.Len())

	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	return ret
}

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func SetFieldValByName(obj interface{}, field string, val interface{}) error {
	if obj == nil {
		return errors.New(`object is null`)
	}
	objType := reflect.ValueOf(obj)
	if objType.Kind() != reflect.Ptr {
		return fmt.Errorf("not ptr; is %T", objType)
	}
	objStruct := objType.Elem()
	if objStruct.Kind() != reflect.Struct {
		return fmt.Errorf("not struct; is %T", objStruct)
	}
	objField := objStruct.FieldByName(field)
	if objField.CanSet() && objField.Kind() == reflect.ValueOf(val).Kind() {
		objField.Set(reflect.ValueOf(val))
		return nil
	} else {
		return fmt.Errorf(`filed can't be set or the val type is invalid %T, %T`, objField.CanSet(), reflect.ValueOf(val).Kind())
	}

}
