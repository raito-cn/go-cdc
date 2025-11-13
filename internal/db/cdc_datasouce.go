package db

import (
	"fmt"
	"go-cdc/pkg/config"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	CDCDataSource *gorm.DB
	cdcOnce       sync.Once
	autoTables    []interface{}
)

func AutoTable(table interface{}) {
	autoTables = append(autoTables, table)
}

func InitCDCDataSource() *gorm.DB {
	cfg := config.Cnf.CDCDataSource
	cdcOnce.Do(func() {
		if cfg.Type == "mysql" {
			dsn := GetMysqlDsn(cfg)
			db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
				Logger: logger.New(
					log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
					logger.Config{
						SlowThreshold:             2000 * time.Millisecond, // 慢 SQL 阈值
						LogLevel:                  logger.Info,             // 日志等级
						IgnoreRecordNotFoundError: true,                    // 忽略 record not found 错误
						Colorful:                  true,                    // 彩色输出
					},
				),
			})
			if err != nil {
				panic(err)
			}
			err = db.AutoMigrate(autoTables...)
			if err != nil {
				panic(err)
			}
			CDCDataSource = db
		} else {
			panic("CDCDataSource type must be mysql")
		}
	})
	return CDCDataSource
}

func GetMysqlDsn(cfg *config.DataSourceConfig) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	str := ""
	for k, v := range cfg.Params {
		str += fmt.Sprintf("%s=%s&", k, v)
	}
	str = strings.TrimSuffix(str, "&")
	if str != "" {
		dsn += "?" + str
	}
	return dsn
}
