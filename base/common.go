package base

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/zogyi/go-web-archetype/util"
	"gopkg.in/guregu/null.v3"
	"strings"
)

const (
	FixedColumnCreateBy   string = `create_by`
	FixedColumnUpdateBy   string = `update_by`
	FixedColumnCreateTime string = `create_time`
	FixedColumnUpdateTime string = `update_time`
	FixedColumnDel        string = `del`
	TagArchType           string = `archType`
	TagPrimaryKey         string = `primaryKey`
	TagAutoFilled         string = `autoFill`
	DefaultUsername       string = `system`

	Insert Operation   = `insert`
	Update Operation   = `update`
	Delete Operation   = `delete`
	Select Operation   = `select`
	ASC    OrderByType = `ASC`
	DESC   OrderByType = `DESC`
)

type Operation string
type OrderByType string

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

const ()

type FieldInfo struct {
	Field        string
	JSONField    string
	TableField   string
	Type         string
	IsLogicDel   bool
	IsPrimaryKey bool
	AutoFilled   bool
}

type OrderBy struct {
	JSONFields []string    `json:"fields"`
	columns    []string    `json:"-"`
	OrderType  OrderByType `json:"orderType"`
}

func (ob *OrderBy) SetColumn(columns []string) {
	ob.columns = columns
}

func (ob *OrderBy) ToSql() string {
	if ob.columns != nil && len(ob.columns) > 0 && strings.TrimSpace(string(ob.OrderType)) != `` {
		return fmt.Sprintf(`%s %s`, strings.Join(ob.columns, `,`), ob.OrderType)
	}
	return ``
}

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

func SetQueryWrapper(ctx context.Context, queryWrapper *ExtraQueryWrapper) context.Context {
	return context.WithValue(ctx, `queryWrapper`, queryWrapper)
}

func ExtractQueryWrapper(ctx context.Context) (tx *ExtraQueryWrapper, ok bool) {
	if ctx.Value(`queryWrapper`) != nil {
		if tx, ok = ctx.Value(`queryWrapper`).(*ExtraQueryWrapper); ok {
			return
		}
	}
	return
}
