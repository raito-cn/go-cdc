package cannal

import (
	"context"
	"database/sql"
	"fmt"
	"go-cdc/internal/log"
	"go-cdc/internal/model"
	"go-cdc/internal/syncdb"
	"go-cdc/pkg/config"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// FullAmountService 全量同步服务
type FullAmountService struct {
	ds         map[string]*syncdb.DataSourceHolder
	dispatcher ChannelDispatcher
	snapshot   SnapshotReader
	consumer   Consumer
	eg         *errgroup.Group
}

// NewFullAmountService 创建全量同步服务
func NewFullAmountService(ds map[string]*syncdb.DataSourceHolder) *FullAmountService {
	ch := make(chan map[string]interface{}, 1000)
	eg, ctx := errgroup.WithContext(context.Background())
	return &FullAmountService{
		ds:         ds,
		dispatcher: ChannelDispatcher{ch: ch, ctx: ctx},
		snapshot:   SnapshotReader{chunkSize: 100, concurrency: 10},
		consumer: Consumer{
			eventConsumer: ConsoleConsumer{},
			ch:            ch,
			ctx:           ctx,
		},
		eg: eg,
	}
}

// Run 全量同步服务执行入口
func (s *FullAmountService) Run() error {
	for _, holder := range s.ds {
		if !holder.IsMysql() {
			continue
		}
		if err := s.handleSource(holder); err != nil {
			return err
		}
	}
	return nil
}

// handleSource 执行主流程
func (s *FullAmountService) handleSource(holder *syncdb.DataSourceHolder) error {
	parser := FilterRuleParser{
		rule: holder.Config.ParseFilterConfig(),
	}

	schemas, err := parser.LoadAndFilterSchemas(*holder)
	if err != nil {
		return err
	}
	tables, err := parser.LoadAndFilterTables(*holder, schemas)
	if err != nil {
		return err
	}
	go s.consumer.Run()
	if err := s.snapshot.ReadAll(s.eg, *holder, tables, &s.dispatcher); err != nil {
		return err
	}
	defer close(s.dispatcher.ch)
	return nil
}

// FilterRuleParser 全量或增量同步过滤库和表的规则解析器
type FilterRuleParser struct {
	rule *config.FilterRule
}

func (f FilterRuleParser) LoadAndFilterSchemas(holder syncdb.DataSourceHolder) ([]string, error) {
	schemas, err := holder.Source.ListSchemas(holder.Source.GetDataSourceTemplate())
	if err != nil {
		return nil, err
	}
	return f.rule.AllowSchemas(schemas), nil
}
func (f FilterRuleParser) LoadAndFilterTables(holder syncdb.DataSourceHolder, schemas []string) (map[string][]string, error) {
	tables, err := holder.Source.ListTables(holder.Source.GetDataSourceTemplate(), schemas...)
	if err != nil {
		return nil, err
	}

	filters := make(map[string][]string)
	for schema, list := range tables {
		for _, t := range list {
			if f.rule.Allow(schema, t) {
				filters[schema] = append(filters[schema], t)
			}
		}
	}
	return filters, nil
}

// SnapshotReader 数据事务快照阅读器
type SnapshotReader struct {
	chunkSize   int
	concurrency int
}

func (sr SnapshotReader) ReadAll(eg *errgroup.Group, holder syncdb.DataSourceHolder, tables map[string][]string, dispatch EventDispatcher) error {
	sem := make(chan struct{}, sr.concurrency)
	for schema, list := range tables {
		for _, table := range list {
			sc, tb := schema, table
			sem <- struct{}{}

			eg.Go(func() error {
				defer func() { <-sem }()
				err := sr.readOneTable(holder, sc, tb, dispatch)
				if err != nil {
					if err := dispatch.Rollback(sc, tb, err); err != nil {
						log.Log.Error("dispatch rollback error", zap.String("schema", sc), zap.String("table", tb), zap.Error(err))
						return err
					}
				}
				return err
			})
		}
	}

	return eg.Wait()
}

func (sr SnapshotReader) readOneTable(holder syncdb.DataSourceHolder, sc, tb string, dispatcher EventDispatcher) error {
	snap, err := holder.Source.BeginTransactionSnapshot()
	if err != nil {
		log.Log.Error("begin snapshot error", zap.String("schema", sc), zap.String("table", tb), zap.Error(err))
		return err
	}
	tx := snap.Tx
	defer model.GetTableMetaService().SaveOrUpdateTableMeta(holder.Config.ID, sc, tb, model.ParseGTID(snap.Pos.(map[string][]string)))
	defer func() {
		_ = tx.Commit()
	}()
	// 1. 获取表的建表语句
	ddl, err := holder.Source.GetTableDDL(tx, sc, tb)
	if err != nil {
		log.Log.Error("get table ddl error", zap.String("schema", sc), zap.String("table", tb), zap.Error(err))
		return err
	}
	if err = dispatcher.DDL(sc, tb, ddl); err != nil {
		log.Log.Error("dispatch ddl error", zap.String("schema", sc), zap.String("table", tb), zap.Error(err))
		return err
	}
	// 2. 获取表的主键
	keys, err := holder.Source.GetTablePrimaryKeys(tx, sc, tb)
	if err != nil {
		log.Log.Error("get table primary keys error", zap.String("schema", sc), zap.String("table", tb), zap.Error(err))
		return err
	}
	lastPK := make(map[string]interface{}, len(keys))
	for _, key := range keys {
		lastPK[key] = nil
	}
	// 3. 分块读取数据
	total, err := sr.countRows(tx, sc, tb)
	if err != nil {
		log.Log.Error("count rows error", zap.String("schema", sc), zap.String("table", tb), zap.Error(err))
		return err
	}

	for i := 0; i < *total; i += sr.chunkSize {
		rows, newPK, err := holder.Source.FetchTableChunk(tx, sc, tb, lastPK, sr.chunkSize)
		if err != nil {
			log.Log.Error("fetch table chunk error", zap.String("schema", sc), zap.String("table", tb), zap.Error(err))
			return err
		}
		if err := dispatcher.Data(sc, tb, rows); err != nil {
			log.Log.Error("dispatch data error", zap.String("schema", sc), zap.String("table", tb), zap.Error(err))
			return err
		}
		lastPK = newPK
	}
	if err := dispatcher.End(sc, tb, snap.Pos); err != nil {
		log.Log.Error("dispatch end error", zap.String("schema", sc), zap.String("table", tb), zap.Error(err))
		return err
	}
	return nil
}

func (sr SnapshotReader) countRows(tx *sql.Tx, sc, tb string) (*int, error) {
	query := fmt.Sprintf("select count(*) from `%s`.`%s`", sc, tb)
	var count int
	if err := tx.QueryRow(query).Scan(&count); err != nil {
		log.Log.Error("count rows error", zap.String("schema", sc), zap.String("table", tb), zap.Error(err))
		return nil, err
	}
	return &count, nil
}

type Consumer struct {
	ch            <-chan map[string]interface{}
	eventConsumer EventConsumer
	ctx           context.Context
}

func (c *Consumer) Run() {
	for {
		select {
		case msg, ok := <-c.ch:
			if !ok {
				return
			}
			if err := c.eventConsumer.Consume(msg); err != nil {
				log.Log.Error("consume message failed", zap.Error(err))
			}
		case <-c.ctx.Done():
			return
		}
	}
}
