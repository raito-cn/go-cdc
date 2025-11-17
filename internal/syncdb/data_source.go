package syncdb

import (
	"database/sql"
	"fmt"
	"go-cdc/internal/db"
	"go-cdc/internal/model"
	"go-cdc/pkg/config"
	"strings"
	"sync"
)

type DataSource interface {
	// ListSchemas 获取数据库所有 schema 名称
	ListSchemas(db *sql.DB) ([]string, error)

	// ListTables 获取数据库所有表名
	ListTables(db *sql.DB, schemas ...string) (map[string][]string, error)

	// GetTableDDL 获取表的建表DDL
	GetTableDDL(tx *sql.Tx, schema string, tables string) (string, error)

	// GetTablePrimaryKeys 获取表的主键
	GetTablePrimaryKeys(tx *sql.Tx, schema string, table string) ([]string, error)

	// FetchTableChunk 分块抓取表数据，chunkSize 可调
	FetchTableChunk(tx *sql.Tx, schema, table string, lastPK map[string]interface{}, chunkSize int) (data []map[string]interface{}, newLastPK map[string]interface{}, err error)

	// BeginTransactionSnapshot 开启事务快照
	BeginTransactionSnapshot() (*TxSnapshot, error)

	// GetDataSourceTemplate 获取数据库连接
	GetDataSourceTemplate() *sql.DB
}

type TxSnapshot struct {
	DB  *sql.DB     // 事务绑定的数据库连接
	Tx  *sql.Tx     // 事务对象
	Pos interface{} // 当前事务执行前的唯一标识，为增量做准备
}

type DataSourceHolder struct {
	ID     uint32
	Source DataSource
	Config *config.DataSourceConfig
}

func (h DataSourceHolder) IsMysql() bool {
	return strings.ToLower(h.Config.Type) == "mysql"
}

var (
	DataSourceMap map[string]*DataSourceHolder
	once          sync.Once
)

// BinlogInitializer 获取全量同步前binlog位置 为增量做准备
type BinlogInitializer struct{}

func (b BinlogInitializer) Init(db *sql.DB) (map[string]interface{}, error) {
	query := "show master status"
	var file, pos, do, ignore, gtid string
	err := db.QueryRow(query).Scan(&file, &pos, &do, &ignore, &gtid)
	if err != nil {
		return nil, fmt.Errorf("BinlogInitializer.Init err: %w", err)
	}
	split := strings.Split(strings.Replace(gtid, "\n", "", -1), ",")
	m := make(map[string]interface{}, len(split))
	for _, str := range split {
		item := strings.Split(str, ":")
		k, v := item[0], item[1]
		m[k] = v
	}
	return m, nil
}

func InitOrGetDataSource() map[string]*DataSourceHolder {
	once.Do(func() {
		binlog := BinlogInitializer{}
		dataSourceConfigs := config.Cnf.DataSourceConfigs
		DataSourceMap = make(map[string]*DataSourceHolder)
		for i, cfg := range dataSourceConfigs {
			fmt.Printf("%s:%d/%s type=%s user=%s params=%v\n", cfg.Host, cfg.Port, cfg.Database, cfg.Type, cfg.User, cfg.Params)
			if cfg.Type == "mysql" {
				dsn := db.GetMysqlDsn(cfg)

				mysqlDB, err := sql.Open("mysql", dsn)
				if err != nil {
					panic(fmt.Errorf("failed to open mysql %s:%s: %v", cfg.Host, cfg.Database, err))
				}

				if err := mysqlDB.Ping(); err != nil {
					panic(fmt.Errorf("failed to ping mysql %s:%s: %v", cfg.Host, cfg.Database, err))
				}
				source := NewMysqlDataSource(mysqlDB)
				binlogPos, err := binlog.Init(mysqlDB)
				if err != nil {
					panic(fmt.Errorf("failed to init mysql %s:%s: %v", cfg.Host, cfg.Database, err))
				}
				source.LastBinlogPos = &binlogPos
				model.GetTableMetaService().SavaOrUpdateCDCMeta(cfg.ID, cfg.Type, binlogPos)
				DataSourceMap[cfg.ID] = &DataSourceHolder{
					ID:     uint32(i + 1),
					Source: source,
					Config: cfg,
				}
			}
		}
	})
	return DataSourceMap
}
