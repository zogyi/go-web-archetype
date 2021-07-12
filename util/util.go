package util

import (
	"errors"
	"gopkg.in/guregu/null.v3"
	"reflect"
	"strings"
	"time"
)

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

type CommonFields struct {
	CreateTime MyNullTime  `json:"create_time" db:"create_time"`
	CreatorBy  null.String `json:"create_by" db:"create_by"`
	UpdateTime MyNullTime  `json:"update_time" db:"update_time"`
	UpdateBy   null.String `json:"update_by" db:"update_by"`
	Del        null.Bool   `db:"del" json:"-"`
}

type Pagination struct {
	PageSize    uint64 `form:"pageSize"`
	CurrentPage uint64 `form:"currentPage"`
	Total       uint64
}


func CreateObjFromInterface(interf interface{}) reflect.Value {
	currentType := reflect.TypeOf(interf)
	if currentType.Kind() != reflect.Struct {
		panic(`the interface should be an struct non pointer`)
	}
	return reflect.New(currentType)
}