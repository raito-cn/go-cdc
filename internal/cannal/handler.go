package cannal

//
//// 负责监听canal事件 并转成Event
//
//import (
//	"context"
//	"fmt"
//	"go-cdc/internal/model"
//	"strconv"
//	"time"
//
//	mg "github.com/go-mysql-org/go-mysql/mysql"
//	rp "github.com/go-mysql-org/go-mysql/replication"
//)
//
//func test(h *Handler) {
//	Cfg := rp.BinlogSyncerConfig{
//		ServerID: 100, // 任意唯一ID
//		Flavor:   "mysql",
//		Host:     "127.0.0.1",
//		Port:     3306,
//		User:     "root",
//		Password: "password",
//	}
//
//	syncer := rp.NewBinlogSyncer(Cfg)
//
//	// 如果用 GTID 同步
//	gtidSet, _ := mg.ParseGTIDSet("mysql", "your_gtid_set_string")
//	streamer, _ := syncer.StartSyncGTID(gtidSet)
//
//	ctx := context.Background() // 永不超时、不可取消
//	ev, err := streamer.GetEvent(ctx)
//	if err != nil {
//		return
//	}
//	var currentGTID string
//	switch e := ev.Event.(type) {
//	case *rp.GTIDEvent:
//		gtid := string(e.SID) + ":" + strconv.FormatInt(e.GNO, 10)
//		fmt.Println(gtid)
//		currentGTID = gtid
//	case *rp.RowsEvent:
//		_ = h.OnRow(currentGTID, e)
//	case *rp.XIDEvent:
//		for _, ev := range h.txBuffer[currentGTID] {
//			h.EventCh <- *ev
//		}
//		delete(h.txBuffer, currentGTID)
//	}
//
//}
//
//type Handler struct {
//	EventCh  chan<- model.Event        // 异步发送下游
//	txBuffer map[string][]*model.Event // key = GID
//}
//
//func NewHandler(ch chan<- model.Event) *Handler {
//	return &Handler{
//		EventCh:  ch,
//		txBuffer: make(map[string][]*model.Event),
//	}
//}
//func (h *Handler) OnRow(gtid string, e *rp.RowsEvent) error {
//	events, err := convertRowEvent(0, e)
//	if err != nil {
//		return err
//	}
//	for _, ev := range events {
//		h.txBuffer[gtid] = append(h.txBuffer[gtid], ev)
//	}
//	return nil
//}
//
//func (h *Handler) OnXID(currentGTID string, e *rp.XIDEvent) {
//	for _, ev := range h.txBuffer[currentGTID] {
//		h.EventCh <- *ev
//	}
//	delete(h.txBuffer, currentGTID)
//}
//
//func convertRowEvent(pos uint32, e *rp.RowsEvent) ([]*model.Event, error) {
//	events := make([]*model.Event, 0, len(e.Rows))
//	for i := 0; i < len(e.Rows); i++ {
//		row := e.Rows[i]
//		data := getRowData(e, row)
//		ev := &model.Event{
//			Table:  string(e.Table.Table),
//			Ts:     time.Now().Unix(),
//			Schema: string(e.Table.Schema),
//		}
//		events = append(events, ev)
//
//		switch e.Type() {
//		case rp.EnumRowsEventTypeInsert:
//			ev.Op = "insert"
//			ev.Data = data
//		case rp.EnumRowsEventTypeUpdate:
//			ev.Op = "update"
//			ev.Before = data
//			i++
//			if i >= len(e.Rows) {
//				return nil, fmt.Errorf("update rows incomplete, missing after row")
//			}
//			after := getRowData(e, e.Rows[i])
//			ev.Data = after
//		case rp.EnumRowsEventTypeDelete:
//			ev.Op = "delete"
//			ev.Before = data
//		case rp.EnumRowsEventTypeUnknown:
//			return nil, fmt.Errorf("unknown event type: %v", e.Type())
//		}
//	}
//	return events, nil
//}
//
//func getRowData(e *rp.RowsEvent, row []interface{}) map[string]interface{} {
//	data := make(map[string]interface{})
//	for idx, colName := range e.Table.ColumnName {
//		data[string(colName)] = row[idx]
//	}
//	return data
//}
