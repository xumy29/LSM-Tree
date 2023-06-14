package config

type Config struct {
	// 单位：ms，每隔一段时间进行compact操作
	CompactInterval int
	// 特殊的value值，当访问到的Elem的value值等于该值时，表示该key被删除
	DeleteValue string
	// 该值为true时，log日志中会有每个键的详细操作记录
	IsTracing bool
}

var (
	defaultConfig *Config
)

func DefaultConfig() *Config {
	if defaultConfig == nil {
		defaultConfig = &Config{
			CompactInterval: 1000,
			DeleteValue:     "DeleteValue",
			IsTracing:       false,
		}
	}
	return defaultConfig
}
