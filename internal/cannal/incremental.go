package cannal

import (
	"context"
	"fmt"
	"go-cdc/internal/log"
	"go-cdc/internal/syncdb"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	rep "github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type IncrementalService interface {
	Run()
	Stop()
	IsRunning() bool
}

type MysqlIncrementalEventHandler interface {
	OnRow(e *rep.RowsEvent) error
	OnDDL(e *rep.QueryEvent) error
	OnGTID(e *rep.GTIDEvent) error
}

type MysqlIncrementalService struct {
	Cfg          *rep.BinlogSyncerConfig
	Holder       *syncdb.DataSourceHolder
	EventHandler MysqlIncrementalEventHandler
	Running      bool
	lock         sync.Mutex
	LastGTID     *map[string]interface{}
	syncer       *rep.BinlogSyncer
	streamer     *rep.BinlogStreamer
}

func NewMysqlIncrementalService(holder *syncdb.DataSourceHolder, eventHandler MysqlIncrementalEventHandler) (IncrementalService, error) {
	source, ok := holder.Source.(*syncdb.MysqlDataSource)
	if !ok {
		log.Log.Error("Holder.Source is not *syncdb.MysqlDataSource", zap.Any("Holder", holder))
		return nil, fmt.Errorf("Holder.Source is not *syncdb.MysqlDataSource")
	}
	cfg := holder.Config
	binlogCfg := &rep.BinlogSyncerConfig{
		ServerID: holder.ID,
		Host:     cfg.Host,
		Port:     uint16(cfg.Port),
		User:     cfg.User,
		Password: cfg.Password,
	}

	service := &MysqlIncrementalService{
		Cfg:          binlogCfg,
		Holder:       holder,
		EventHandler: eventHandler,
		LastGTID:     source.LastBinlogPos,
		Running:      false,
		lock:         sync.Mutex{},
	}

	return service, nil
}

func (service *MysqlIncrementalService) Run() {
	service.lock.Lock()
	if service.Running {
		log.Log.Warn("MysqlIncrementalService is already running")
		service.lock.Unlock()
		return
	}
	service.Running = true
	service.lock.Unlock()
	go service.init()
}

// Stop 线程安全地停止服务并关闭 syncer
func (service *MysqlIncrementalService) Stop() {
	service.lock.Lock()
	defer service.lock.Unlock()
	if !service.Running {
		return
	}
	service.Running = false
	// 关闭 syncer 会使 GetEvent 返回 error
	if service.syncer != nil {
		service.syncer.Close()
	}
}

func (service *MysqlIncrementalService) IsRunning() bool {
	return service.Running
}

func (service *MysqlIncrementalService) init() {
	var backoff = 1 * time.Second
	fallbackTimes := 0
	allowFallback := func(err error) bool {
		service.syncer.Close()
		time.Sleep(backoff)
		backoff = min(backoff*2, 30*time.Second)
		fallbackTimes++
		if fallbackTimes > 10 {
			log.Log.Error("MysqlIncrementalService.init: get GTID failed 10 times, exit")
			return false
		} else {
			log.Log.Error("MysqlIncrementalService.init: get GTID failed, retry again", zap.Error(err))
			return true
		}
	}

	for {
		if !service.Running {
			return
		}

		// new syncer per attempt
		service.syncer = rep.NewBinlogSyncer(*service.Cfg)

		// 如果没有GTID，allowFallback -> 从master pos开始
		if service.LastGTID == nil || len(*service.LastGTID) == 0 {
			snapshot, err := service.Holder.Source.BeginTransactionSnapshot()
			if err != nil {
				if allowFallback(err) {
					continue
				} else {
					return
				}
			}
			m := snapshot.Pos.(map[string]interface{})
			service.LastGTID = &m

		}

		// 构建 GTIDSet 字符串
		str := ""
		for k, v := range *service.LastGTID {
			str += fmt.Sprintf("%s:%s,", k, v)
		}
		str = strings.TrimSuffix(str, ",")

		GTIDSet, err := mysql.ParseGTIDSet("mysql", str)
		if err != nil {
			if allowFallback(err) {
				continue
			} else {
				return
			}
		}
		streamer, err := service.syncer.StartSyncGTID(GTIDSet)
		if err != nil {
			if allowFallback(err) {
				continue
			} else {
				return
			}
		}
		service.streamer = streamer

		// 重置 backoff
		backoff = 1 * time.Second
		fallbackTimes = 0

		// 进入事件循环
		service.loop()

		service.syncer.Close()
		service.streamer = nil
		service.syncer = nil

		if !service.Running {
			return
		}

		// 出错重新连接
		time.Sleep(backoff)
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (service *MysqlIncrementalService) loop() {
	defer func() {
		if r := recover(); r != nil {
			log.Log.Error("panic in loop recovered", zap.Any("panic", r))
		}
	}()

	ctx := context.Background()
	for service.Running {
		ev, err := service.streamer.GetEvent(ctx)
		if err != nil {
			log.Log.Error("MysqlIncrementalService.loop: get event failed", zap.Error(err))
			return
		}
		switch e := ev.Event.(type) {
		case *rep.GTIDEvent:
			guuid := uuid.Must(uuid.FromBytes(e.SID[:])).String()
			gno := e.GNO
			service.lock.Lock()
			if service.LastGTID == nil {
				m := make(map[string]interface{})
				service.LastGTID = &m
			}
			m := *service.LastGTID
			m[guuid] = fmt.Sprintf("%d", gno)
			service.lock.Unlock()
			if service.EventHandler != nil {
				if err := service.EventHandler.OnGTID(e); err != nil {
					log.Log.Error("OnGTID handler error", zap.Error(err))
				}
			}

		case *rep.QueryEvent:
			q := string(e.Query)
			up := strings.ToUpper(strings.TrimSpace(q))
			if strings.HasPrefix(up, "CREATE") ||
				strings.HasPrefix(up, "ALTER") ||
				strings.HasPrefix(up, "DROP") ||
				strings.HasPrefix(up, "RENAME") ||
				strings.HasPrefix(up, "TRUNCATE") {
				if service.EventHandler != nil {
					if err := service.EventHandler.OnDDL(e); err != nil {
						log.Log.Error("OnDDL handler error", zap.Error(err))
					}
				}
			}
		case *rep.RowsEvent:
			if service.EventHandler != nil {
				if err := service.EventHandler.OnRow(e); err != nil {
					log.Log.Error("OnRow handler error", zap.Error(err))
				}
			}
		}
	}
}
