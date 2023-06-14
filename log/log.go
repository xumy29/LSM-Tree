package lsmt

import (
	"LSM-Tree/config"
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

/* 一些比debug信息更细的内容用此函数打印，只在需要跟踪某个key时打印，其余时候不打印 */
func Trace(msg string, ctx ...interface{}) {
	// 下面语句默认不调用，只在需要较详细的debug信息时调用
	if config.DefaultConfig().IsTracing {
		msg = "==Trace==" + msg
		log.Debug(msg, ctx)
	}
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
