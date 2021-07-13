package log

import (
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger
var cleanUp func()

func InitLog(path string, logLevel string) {
	hook := lumberjack.Logger{
		Filename:   path,  //日志文件路径
		MaxSize:    1024, //最大字节
		MaxAge:     30,
		MaxBackups: 7,
	}
	w := zapcore.AddSync(&hook)
	var level zapcore.Level
	switch logLevel {
		case "debug":
			level = zap.DebugLevel
		case "info":
			level= zap.InfoLevel
		case "error":
			level = zap.ErrorLevel
		default:
			level = zap.InfoLevel
	}

	encodingConfig := zap.NewProductionEncoderConfig()
	encodingConfig.TimeKey = "timestamp"
	encodingConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(encodingConfig), w, level)
	logger = zap.New(core)
	cleanUp = zap.ReplaceGlobals(logger)
}

func CleanUp() {
	logger.Sync()
	cleanUp()
}
