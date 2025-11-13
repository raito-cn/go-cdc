package config

import (
	"os"
	"strings"
	"sync"

	"github.com/pelletier/go-toml/v2"
)

type DataSourceConfig struct {
	ID       string                   `toml:"id"`
	Type     string                   `toml:"type"`
	Host     string                   `toml:"host"`
	Port     int                      `toml:"port"`
	User     string                   `toml:"user"`
	Password string                   `toml:"password"`
	Database string                   `toml:"database"`
	Params   map[string]string        `toml:"params"`
	Global   *FilterConfig            `toml:"global_filter"`
	Schemas  map[string]*FilterConfig `toml:"schema_filters"`
}
type FilterConfig struct {
	IncludeSchemas string `toml:"include_schemas"`
	IncludeTables  string `toml:"include_tables"`
	ExcludeTables  string `toml:"exclude_tables"`
}

type CdcConfig struct {
	DataSourceConfigs []*DataSourceConfig `toml:"DATASOURCE"`
	CDCDataSource     *DataSourceConfig   `toml:"CDC_DATASOURCE"`
}

var (
	Cnf    *CdcConfig
	once   sync.Once
	cfgErr error
)

func LoadConfig(configPath string) (*CdcConfig, error) {
	once.Do(func() {
		data, err := os.ReadFile(configPath)
		if err != nil {
			cfgErr = err
			return
		}
		cfg := &CdcConfig{}
		if err := toml.Unmarshal(data, cfg); err != nil {
			cfgErr = err
			return
		}
		Cnf = cfg
	})
	return Cnf, cfgErr
}

type FilterRule struct {
	Global   *FilterPattern
	BySchema map[string]*FilterPattern
}

type FilterPattern struct {
	IncludeSchemas []string
	IncludeTables  []string
	ExcludeTables  []string
}

func (cfg *DataSourceConfig) ParseFilterConfig() *FilterRule {
	rule := &FilterRule{BySchema: make(map[string]*FilterPattern)}

	if cfg.Global != nil {
		rule.Global = toPattern(cfg.Global)
	}
	for schema, fc := range cfg.Schemas {
		rule.BySchema[schema] = toPattern(fc)
	}
	return rule
}

func toPattern(fc *FilterConfig) *FilterPattern {
	return &FilterPattern{
		IncludeSchemas: splitComma(fc.IncludeSchemas),
		IncludeTables:  splitComma(fc.IncludeTables),
		ExcludeTables:  splitComma(fc.ExcludeTables),
	}
}

func splitComma(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func (r *FilterRule) AllowSchemas(schemas []string) []string {
	var ans []string
	for _, schema := range schemas {
		if r.Allow(schema, "") {
			ans = append(ans, schema)
		}
	}
	return ans
}

func (r *FilterRule) Allow(schema, table string) bool {
	// schema 级别规则（优先）
	if sr, ok := r.BySchema[schema]; ok {
		return sr.allow("", table)
	}

	// 全局规则（退回）
	if r.Global != nil {
		return r.Global.allow(schema, table)
	}

	// 无配置默认全部允许
	return true
}

func (p *FilterPattern) allow(schema, table string) bool {
	// schema 限制
	if schema != "" {
		if len(p.IncludeSchemas) > 0 && !contains(p.IncludeSchemas, schema) {
			return false
		}
	}
	if table != "" {
		// 排除表优先
		if containsPrefix(p.ExcludeTables, table) {
			return false
		}
		// 若设置 include_tables，仅允许其中
		if len(p.IncludeTables) > 0 {
			return containsPrefix(p.IncludeTables, table)
		}
	}
	return true
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func containsPrefix(list []string, s string) bool {
	for _, v := range list {
		if strings.HasSuffix(v, "_") && strings.HasPrefix(s, strings.TrimSuffix(v, "_")) {
			return true // 支持前缀匹配
		}
		if v == s {
			return true
		}
	}
	return false
}
