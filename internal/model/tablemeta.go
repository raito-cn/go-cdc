package model

import (
	"encoding/json"
	"errors"
	"go-cdc/internal/db"
	"go-cdc/internal/log"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type CDCMeta struct {
	ID             int64  `gorm:"column:id;primaryKey;autoIncrement:true;type:bigint;comment:CDC元数据ID"`
	DataSourceID   string `gorm:"column:data_source_id;type:varchar(50);comment:数据源ID;uniqueIndex:uniq_datasource_id"`
	DataSourceType string `gorm:"column:data_source_type;type:varchar(50);comment:数据源类型"`
	LastPos        string `gorm:"column:last_pos;type:json;comment:数据源CDC增量更新最新位置"`
}

func (CDCMeta) TableName() string {
	return "go_cdc_meta"
}

type TableMeta struct {
	ID           int64  `gorm:"column:id;primarykey;autoIncrement:true;columnType:bigint;comment:CDC同步表元数据ID"`
	DataSourceID string `gorm:"column:data_source_id;type:varchar(50);comment:数据源ID;uniqueIndex:uniq_table"`
	Sc           string `gorm:"column:sc;type:varchar(50);comment:数据库名;uniqueIndex:uniq_table"`
	Tb           string `gorm:"column:tb;type:varchar(50);comment:表名;uniqueIndex:uniq_table"`
	LastPos      string `gorm:"column:last_pos;type:json;comment:表CDC增量更新最新位置"`
}

func (TableMeta) TableName() string {
	return "go_cdc_table_meta"
}

func init() {
	db.AutoTable(&CDCMeta{})
	db.AutoTable(&TableMeta{})
}

var (
	tableMetaServiceOnce sync.Once
	tableMetaService     TableMetaService
)

type TableMetaService struct{}

func GetTableMetaService() TableMetaService {
	tableMetaServiceOnce.Do(func() {
		tableMetaService = TableMetaService{}
	})
	return tableMetaService
}

func (service TableMetaService) SaveOrUpdateTableMeta(datasourceID, sc, tb string, lastPos interface{}) {
	b, _ := json.Marshal(lastPos)
	meta := TableMeta{
		DataSourceID: datasourceID,
		Sc:           sc,
		Tb:           tb,
		LastPos:      string(b),
	}
	var existing TableMeta
	t := db.CDCDataSource.Model(&TableMeta{})
	err := t.Where("sc = ? and tb = ? and data_source_id = ?", sc, tb, datasourceID).First(&existing).Error
	if err != nil {
		if errors.Is(gorm.ErrRecordNotFound, err) {
			// 不存在则插入
			if err := db.CDCDataSource.Create(&meta).Error; err != nil {
				log.Log.Error("insert table meta failed", zap.Error(err))
			}
		} else {
			log.Log.Error("query table meta failed", zap.Error(err))
		}
	} else {
		// 存在则更新 LastPos
		if err := db.CDCDataSource.Model(&existing).Update("last_pos", meta.LastPos).Error; err != nil {
			log.Log.Error("update table meta failed", zap.Error(err))
		}
	}
}

func (service TableMetaService) SavaOrUpdateCDCMeta(dataSourceID, dataSourceType string, lastPos interface{}) {
	b, _ := json.Marshal(lastPos)
	meta := &CDCMeta{
		DataSourceID:   dataSourceID,
		LastPos:        string(b),
		DataSourceType: dataSourceType,
	}
	var existing CDCMeta
	t := db.CDCDataSource.Model(&CDCMeta{})
	err := t.Where("data_source_id = ?", dataSourceID).First(&existing).Error
	if err != nil {
		if errors.Is(gorm.ErrRecordNotFound, err) {
			// 不存在则插入
			if err := db.CDCDataSource.Create(&meta).Error; err != nil {
				log.Log.Error("insert cdc meta failed", zap.Error(err))
			}
		} else {
			log.Log.Error("query cdc meta failed", zap.Error(err))
		}
	} else {
		// 存在则更新 LastPos
		if err := db.CDCDataSource.Model(&existing).Updates(map[string]interface{}{
			"last_pos":         meta.LastPos,
			"data_source_type": meta.DataSourceType,
		}).Error; err != nil {
			log.Log.Error("update cdc meta failed", zap.Error(err))
		}
	}
}
