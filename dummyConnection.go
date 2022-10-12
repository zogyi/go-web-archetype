package go_web_archetype

import (
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

//DummyConnection a fake database connection for testing
type DummyConnection struct {
}

func (dc *DummyConnection) Preparex(query string) (*sqlx.Stmt, error) {
	zap.L().Debug(`execute method in a dummy connection`)
	return nil, nil
}
func (dc *DummyConnection) Select(dest interface{}, query string, args ...interface{}) error {
	zap.L().Debug(`execute method in a dummy connection`)
	return nil
}
func (dc *DummyConnection) Get(dest interface{}, query string, args ...interface{}) error {
	zap.L().Debug(`execute method in a dummy connection`)
	return nil
}
