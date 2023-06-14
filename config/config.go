package config

import "math"

type Config struct {
	// 特殊的value值，当访问到的Elem的value值等于该值时，表示该key被删除
	DeleteValue string
	// 该值为true时，log日志中会有每个键的详细操作记录
	IsTracing bool

	// 磁盘文件
	// 每隔多少个元素建立一个索引节点
	IndexDistance int
	// 内存中的树的能存储的最大键值对个数，容量满时flush到一个level-0文件，清空内存中的树
	ElemCnt2Flush int
	// level-0文件数量上限，达到上限时向level-1合并
	MaxLevel0FileCnt int
	// Level-1+ 每个文件的最大体积，单位 MB
	LevelLFileSize int
	// level-1+ 总体积上限，达到上限时向level-L+1合并
	LevelLSize2Compact func(int) int
}

var (
	defaultConfig *Config
)

func DefaultConfig() *Config {
	if defaultConfig == nil {
		defaultConfig = &Config{
			DeleteValue:      "DeleteValue",
			IsTracing:        false,
			IndexDistance:    10,
			ElemCnt2Flush:    10000,
			MaxLevel0FileCnt: 4,
			LevelLFileSize:   1,
			LevelLSize2Compact: func(lvl int) int {
				return int(math.Pow10(lvl))
			},
		}
	}
	return defaultConfig
}
