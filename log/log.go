package lsmt

import (
	"os"
	"path/filepath"

	log "github.com/inconshreveable/log15"
)

var (
	logFile = "logfile.log"
	Logger  log.Logger
)

func init() {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	logFile = filepath.Join(wd, logFile)
	// 创建日志输出文件
	_, err = os.Create(logFile)
	if err != nil {
		panic(err)
	}
	// 设置日志
	handler, err := log.FileHandler(logFile, log.LogfmtFormat())
	if err != nil {
		panic(err)
	}
	log.Root().SetHandler(log.CallerFileHandler(handler))
	// 使用日志
	log.Debug("第一条日志")

	Logger = log.Root()
}

// func Debug(msg string, ctx ...interface{}) {
// 	log.Debug(msg, ctx)
// }

// func Info(msg string, ctx ...interface{}) {
// 	log.Info(msg, ctx)
// }

// func Warn(msg string, ctx ...interface{}) {
// 	log.Warn(msg, ctx)
// }

// func Error(msg string, ctx ...interface{}) {
// 	log.Error(msg, ctx)
// }

// func Crit(msg string, ctx ...interface{}) {
// 	log.Crit(msg, ctx)
// }
