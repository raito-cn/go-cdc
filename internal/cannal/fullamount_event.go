package cannal

import (
	"context"
	"go-cdc/internal/log"

	"go.uber.org/zap"
)

// EventDispatcher 全量同步事件分发器
type EventDispatcher interface {
	DDL(schema, table string, ddl interface{}) error
	Data(schema, table string, rows []map[string]interface{}) error
	End(schema, table string, pos interface{}) error
	Rollback(schema, table string, err error) error
}

// ChannelDispatcher 事件分发器
type ChannelDispatcher struct {
	ch  chan<- map[string]interface{} // 事件通道
	ctx context.Context               // 协程控制信号和数据载体
}

func (cd *ChannelDispatcher) DDL(schema, table string, ddl interface{}) error {
	msg := map[string]interface{}{
		"schema": schema,
		"table":  table,
		"data":   ddl,
		"type":   "create_table",
	}
	select {
	case cd.ch <- msg:
		return nil
	case <-cd.ctx.Done():
		return cd.ctx.Err()
	}
}

func (cd *ChannelDispatcher) Data(schema, table string, rows []map[string]interface{}) error {
	msg := map[string]interface{}{
		"schema": schema,
		"table":  table,
		"data":   rows,
		"type":   "insert",
	}
	select {
	case cd.ch <- msg:
		return nil
	case <-cd.ctx.Done():
		return cd.ctx.Err()
	}
}

func (cd *ChannelDispatcher) End(schema, table string, pos interface{}) error {
	msg := map[string]interface{}{
		"schema": schema,
		"table":  table,
		"pos":    pos,
		"type":   "end",
	}
	select {
	case cd.ch <- msg:
		return nil
	case <-cd.ctx.Done():
		return cd.ctx.Err()
	}
}

func (cd *ChannelDispatcher) Rollback(schema, table string, err error) error {
	msg := map[string]interface{}{
		"schema": schema,
		"table":  table,
		"err":    err.Error(),
		"type":   "rollback",
	}

	select {
	case cd.ch <- msg:
		return nil
	case <-cd.ctx.Done():
		return cd.ctx.Err()
	}
}

// EventConsumer 事件消费者
type EventConsumer interface {
	Consume(msg map[string]interface{}) error
}

// ConsoleConsumer 事件控制台消费实现
type ConsoleConsumer struct{}

func (cc ConsoleConsumer) Consume(msg map[string]interface{}) error {
	log.Log.Info("Consume", zap.Any("msg", msg))
	return nil
}
