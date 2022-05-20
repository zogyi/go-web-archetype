package go_web_archetype

import (
	"fmt"
	"github.com/zogyi/go-web-archetype/util"
	"gopkg.in/guregu/null.v3"
	"strings"
)

type Operation string
type OrderByType string

const (
	Insert Operation   = `insert`
	Update Operation   = `update`
	Delete Operation   = `delete`
	Select Operation   = `select`
	ASC    OrderByType = `ASC`
	DESC   OrderByType = `DESC`
)

type CommonFields struct {
	Id         null.Int        `json:"id" archType:"primaryKey,autoFill"`
	CreateTime util.MyNullTime `json:"createTime" db:"create_time" archType:"autoFill"`
	CreatorBy  null.String     `json:"createBy" db:"create_by" archType:"autoFill"`
	UpdateTime util.MyNullTime `json:"updateTime" db:"update_time" archType:"autoFill"`
	UpdateBy   null.String     `json:"updateBy" db:"update_by" archType:"autoFill"`
}

type CommonDel struct {
	Del null.Bool `json:"-" db:"del" archType:"autoFill"`
}

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
