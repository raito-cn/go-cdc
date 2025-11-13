package model

type Event struct {
	DataSource string                 // 数据源ID
	Table      string                 // table name
	Op         string                 // insert, update, delete
	Data       map[string]interface{} // insert or update after data snapshot
	Before     map[string]interface{} // update or delete before data snapshot
	Ts         int64                  // unix timestamp
	Pos        string                 // position
	Schema     string                 // schema name
}
