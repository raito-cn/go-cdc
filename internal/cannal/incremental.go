package cannal

import (
	"context"
	"fmt"
	"go-cdc/internal/log"
	"go-cdc/internal/model"
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

type MySQLIncrementalEventHandler interface {
	OnRow(e *rep.RowsEvent) error
	OnDDL(e *rep.QueryEvent) error
	OnGTID(e *rep.GTIDEvent) error
}

type MySQLIncrementalService struct {
	Cfg          *rep.BinlogSyncerConfig
	Holder       *syncdb.DataSourceHolder
	EventHandler MySQLIncrementalEventHandler
	Running      bool
	lock         sync.Mutex
	LastGTID     *model.GTID
	syncer       *rep.BinlogSyncer
	streamer     *rep.BinlogStreamer
}

func NewMySQLIncrementalService(holder *syncdb.DataSourceHolder) (IncrementalService, error) {
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

	service := &MySQLIncrementalService{
		Cfg:          binlogCfg,
		Holder:       holder,
		EventHandler: &MySQLIncrementalImpl{Holder: holder},
		LastGTID:     source.LastGTID,
		Running:      false,
		lock:         sync.Mutex{},
	}

	return service, nil
}

func (service *MySQLIncrementalService) Run() {
	service.lock.Lock()
	if service.Running {
		log.Log.Warn("MySQLIncrementalService is already running")
		service.lock.Unlock()
		return
	}
	service.Running = true
	service.lock.Unlock()
	go service.init()
}

// Stop 线程安全地停止服务并关闭 syncer
func (service *MySQLIncrementalService) Stop() {
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

func (service *MySQLIncrementalService) IsRunning() bool {
	return service.Running
}

func (service *MySQLIncrementalService) init() {
	var backoff = 1 * time.Second
	fallbackTimes := 0
	allowFallback := func(err error) bool {
		service.syncer.Close()
		time.Sleep(backoff)
		backoff = min(backoff*2, 30*time.Second)
		fallbackTimes++
		if fallbackTimes > 10 {
			log.Log.Error("MySQLIncrementalService.init: get GTID failed 10 times, exit")
			return false
		} else {
			log.Log.Error("MySQLIncrementalService.init: get GTID failed, retry again", zap.Error(err))
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
			m := snapshot.Pos.(map[string][]string)
			service.LastGTID = model.ParseGTID(m)
		}

		// 构建 GTIDSet 字符串
		str := service.LastGTID.String()

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

func (service *MySQLIncrementalService) loop() {
	defer func() {
		if r := recover(); r != nil {
			log.Log.Error("panic in loop recovered", zap.Any("panic", r))
		}
	}()

	ctx := context.Background()
	for service.Running {
		ev, err := service.streamer.GetEvent(ctx)
		if err != nil {
			log.Log.Error("MySQLIncrementalService.loop: get event failed", zap.Error(err))
			return
		}
		switch e := ev.Event.(type) {
		case *rep.GTIDEvent:
			guuid := uuid.Must(uuid.FromBytes(e.SID[:])).String()
			gno := e.GNO
			service.lock.Lock()
			if service.LastGTID == nil {
				service.LastGTID = &model.GTID{}
			}
			service.LastGTID.SetGTID(guuid, gno)
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
