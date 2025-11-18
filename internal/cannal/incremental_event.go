package cannal

import (
	"go-cdc/internal/log"
	"go-cdc/internal/syncdb"

	rep "github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"
)

type MySQLIncrementalImpl struct {
	Holder *syncdb.DataSourceHolder
}

func (impl *MySQLIncrementalImpl) OnRow(e *rep.RowsEvent) error {
	table := string(e.Table.Table)
	schema := string(e.Table.Schema)
	rule := impl.Holder.Config.FilterRule
	if rule == nil {
		rule = impl.Holder.Config.ParseFilterConfig()
	}
	allow := rule.Allow(schema, table)
	if !allow {
		return nil
	}
	log.Log.Info("allow table", zap.String("schema", schema), zap.String("table", table),
		zap.Any("data", e.Rows), zap.String("type", e.Type().String()))
	return nil
}

func (impl *MySQLIncrementalImpl) OnDDL(e *rep.QueryEvent) error {
	return nil
}

func (impl *MySQLIncrementalImpl) OnGTID(e *rep.GTIDEvent) error {

	return nil
}
