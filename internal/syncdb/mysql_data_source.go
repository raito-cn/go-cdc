package syncdb

import (
	"context"
	"database/sql"
	"fmt"
	"go-cdc/internal/model"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlDataSource struct {
	Db       *sql.DB
	LastGTID *model.GTID
}

func NewMysqlDataSource(db *sql.DB) *MysqlDataSource {
	return &MysqlDataSource{
		Db: db,
	}
}

func (mysql *MysqlDataSource) ListSchemas(db *sql.DB) ([]string, error) {
	query := `
		select schema_name
		from information_schema.schemata
		where schema_name not in ('information_schema', 'mysql', 'performance_schema', 'sys')
	`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	schemas := make([]string, 0)
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

func (mysql *MysqlDataSource) ListTables(db *sql.DB, schemas ...string) (map[string][]string, error) {
	query := `
		select table_schema, table_name
		from information_schema.tables
		where table_type = 'BASE TABLE'
		and table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
	`
	var args []interface{}
	if len(schemas) > 0 {
		placeholders := make([]string, len(schemas))
		for i, s := range schemas {
			placeholders[i] = "?"
			args = append(args, s)
		}
		query += fmt.Sprintf(" and table_schema in (%s)", strings.Join(placeholders, ","))
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	tables := make(map[string][]string)
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, err
		}
		tables[schema] = append(tables[schema], table)
	}
	return tables, nil
}

func (mysql *MysqlDataSource) GetTableDDL(tx *sql.Tx, schema, table string) (string, error) {

	var tableName, ddl string
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	err := tx.QueryRow(query).Scan(&tableName, &ddl)
	if err != nil {
		return "", fmt.Errorf("show create table `%s`.`%s` err: %v", schema, table, err)
	}

	return ddl, nil
}

func (mysql *MysqlDataSource) GetTablePrimaryKeys(tx *sql.Tx, schema, table string) ([]string, error) {
	// 先查询是否有pri 没有的直接放弃同步
	query := `
		select column_name from information_schema.columns where table_name = ? and table_schema = ?
		and column_key = 'PRI'
	`
	rows, err := tx.Query(query, table, schema)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	pris := make([]string, 0)

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		pris = append(pris, columnName)
	}

	if len(pris) == 0 {
		return nil, fmt.Errorf("table %s.%s has no primary key", schema, table)
	}
	return pris, nil
}

func (mysql *MysqlDataSource) FetchTableChunk(tx *sql.Tx, schema, table string, lastPK map[string]interface{}, chunkSize int) (data []map[string]interface{}, newLastPK map[string]interface{}, err error) {
	//pkMap, ok := lastPK.(map[string]string)
	if len(lastPK) == 0 {
		return nil, nil, fmt.Errorf("lastPK is empty")
	}
	var pkCols []string
	var pkVals []interface{}
	flag := true
	for k, v := range lastPK {
		pkCols = append(pkCols, k)
		pkVals = append(pkVals, v)
		if v == nil {
			flag = false
		}
	}
	var query string
	var args []interface{}
	if flag {
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE (%s) > (%s) ORDER BY %s LIMIT ?",
			schema, table,
			strings.Join(pkCols, ","),
			strings.Repeat("?,", len(pkCols)-1)+"?",
			strings.Join(pkCols, ","),
		)
		args = append(pkVals, chunkSize)
	} else {
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY %s LIMIT ?",
			schema, table,
			strings.Join(pkCols, ","),
		)
		args = []interface{}{chunkSize}
	}

	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		row := make(map[string]interface{})
		for i, col := range cols {
			if b, ok := values[i].([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = values[i]
			}
		}
		data = append(data, row)
	}
	if len(data) > 0 {
		for k, v := range data[len(data)-1] {
			_, ok := lastPK[k]
			if ok {
				lastPK[k] = v
			}
		}
		newLastPK = lastPK
	}
	return data, newLastPK, nil
}

func (mysql *MysqlDataSource) getTableGTID(tx *sql.Tx) (map[string][]string, error) {
	query := fmt.Sprintf("SELECT @@GLOBAL.gtid_executed;")
	var str string
	if err := tx.QueryRow(query).Scan(&str); err != nil {
		return nil, err
	}
	split := strings.Split(strings.Replace(str, "\n", "", -1), ",")
	m := make(map[string][]string, len(split))
	for _, str := range split {
		item := strings.Split(str, ":")
		k, v := item[0], item[1:]
		m[k] = v
	}
	return m, nil
}

func (mysql *MysqlDataSource) BeginTransactionSnapshot() (*TxSnapshot, error) {
	tx, err := mysql.Db.BeginTx(context.Background(),
		&sql.TxOptions{
			ReadOnly:  true,
			Isolation: sql.LevelRepeatableRead,
		})
	if err != nil {
		return nil, err
	}
	gtid, err := mysql.getTableGTID(tx)
	if err != nil {
		return nil, err
	}
	snapshot := &TxSnapshot{DB: mysql.Db, Tx: tx, Pos: gtid}
	return snapshot, nil
}

func (mysql *MysqlDataSource) GetDataSourceTemplate() *sql.DB {
	return mysql.Db
}
