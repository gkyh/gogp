package gorp

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var prefix string = "tb_"

type gorpDB struct {
	dbmap       *DbMap
	parent      *gorpDB
	tx          *Transaction
	Query       string
	inCondition string
	params      []interface{}
	Condition   []map[string]interface{}
	OrCondition []map[string]interface{}
	table       string
	field       string
	Offset      int32
	Limit       int32
	sort        string
	group       string
	Err         error
}

func (m *DbMap) SetPrefix(s string) {

	prefix = s
}

func getName(class interface{}) string {

	t := reflect.TypeOf(class)
	str := fmt.Sprintf("%v", t)

	buff := bytes.NewBuffer([]byte{})

	for pos, char := range str {
		if str[pos] != '*' && str[pos] != '[' && str[pos] != ']' {

			buff.WriteRune(char)
		}
	}

	return buff.String()
}

func (m *DbMap) GetTableName(class interface{}) string {

	tName := getName(class)
	for i := range m.tables {
		table := m.tables[i]

		tb := fmt.Sprintf("%v", table.gotype)
		if tb == tName {

			return table.TableName
		}
	}
	return getTable(class)
}

//ad dbMap new month
func (m *DbMap) Model(class interface{}) *gorpDB {

	db := &gorpDB{dbmap: m, parent: nil, tx: nil, Query: "", table: "", Condition: nil, field: "*", Offset: 0, Limit: 0, sort: "", group: ""}

	db.table = m.GetTableName(class)
	return db

}
func (m *DbMap) Table(name string) *gorpDB {

	db := &gorpDB{dbmap: m, parent: nil, tx: nil, Query: "", table: "", Condition: nil, field: "*", Offset: 0, Limit: 0, sort: "", group: ""}

	db.table = name
	return db
}
func (m *DbMap) Where(query string, values ...interface{}) *gorpDB {

	db := &gorpDB{dbmap: m, parent: nil, tx: nil, Query: "", table: "", Condition: nil, field: "*", Offset: 0, Limit: 0, sort: "", group: ""}

	db.Condition = append(db.Condition, map[string]interface{}{"query": query, "args": values})
	return db
}
func (m *DbMap) Tx(tx *Transaction) *gorpDB {

	db := &gorpDB{dbmap: m, parent: nil, tx: nil, Query: "", table: "", Condition: nil, field: "*", Offset: 0, Limit: 0, sort: "", group: ""}
	db.tx = tx
	return db
}

func (m *DbMap) FindById(out, id interface{}) error {

	table := m.GetTableName(out)

	sql := bytes.Buffer{}
	sql.WriteString("SELECT *  FROM ")
	sql.WriteString(table)
	sql.WriteString(" WHERE id=?")

	return m.SelectOne(out, sql.String(), id)
}

func m_type(i interface{}) string {
	switch i.(type) {
	case string:
		return "string"
	case int:
		return "number"
	case int32:
		return "number"
	case int64:
		return "number"
	case float64:
		return "number"
	case []string:
		return "strings"
	default:
		return ""
	}

}
func getTable(class interface{}) string {

	var table string
	ts := reflect.TypeOf(class)
	se := fmt.Sprintf("%v", ts)

	idx := strings.LastIndex(se, ".")
	if idx > 0 {

		idx++
		ss := string([]rune(se)[idx:len(se)])
		table = strings.ToLower(ss)
	} else {
		table = se
	}

	return prefix + table
}

func (db *gorpDB) Select(args string) *gorpDB {

	db.field = args
	return db
}

func (db *gorpDB) Tx(tx *Transaction) *gorpDB {

	db.tx = tx
	return db
}
func (db *gorpDB) Update(field string, values ...interface{}) error {

	sql := bytes.Buffer{}

	sql.WriteString("UPDATE ")
	sql.WriteString(db.table)
	sql.WriteString(" set ")
	sql.WriteString(field)

	sql.WriteString(db.BuildSql())

	if db.tx == nil {
		_, db.Err = db.dbmap.Exec(sql.String(), values...)
	} else {
		_, db.Err = db.tx.Exec(sql.String(), values...)
	}
	/*
		idAff, err := result.RowsAffected()
		if err != nil {

			return err
		}
		//log.Info("id:", idAff)
		if idAff == 0 {

			//log.Error("RowsAffected failed: no found rows")
			return errors.New("RowsAffected failed: no found rows")
		}
	*/
	return db.Err

}

func (db *gorpDB) Delete(class interface{}) error {

	if db.table == "" {
		db.table = db.dbmap.GetTableName(class)
	}
	sql := bytes.Buffer{}

	sql.WriteString("DELETE  FROM ")
	sql.WriteString(db.table)

	sql.WriteString(db.BuildSql())

	if db.tx == nil {

		_, db.Err = db.dbmap.Exec(sql.String())
	} else {
		_, db.Err = db.tx.Exec(sql.String())
	}

	return db.Err
}

func (db *gorpDB) Save(list ...interface{}) error {

	if db.tx == nil {
		return db.dbmap.Insert(list...)
	} else {
		return db.tx.Insert(list...)
	}
}

func (db *gorpDB) Table(name string) *gorpDB {

	db.table = name
	return db
}
func (db *gorpDB) Field(field string) *gorpDB {

	db.field = field
	return db
}
func (db *gorpDB) Sort(key, sort string) *gorpDB {

	db.sort = fmt.Sprintf(" ORDER BY %s %s ", key, sort)
	return db
}
func (db *gorpDB) Page(cur, count int32) *gorpDB {

	start := (cur - 1) * count
	if start < 0 {
		start = 0
	}
	db.Offset = start
	db.Limit = count
	return db
}

func (db *gorpDB) Where(query string, values ...interface{}) *gorpDB {

	db.Condition = append(db.Condition, map[string]interface{}{"query": query, "args": values})
	return db
}
func (db *gorpDB) Or(query string, values ...interface{}) *gorpDB {

	db.OrCondition = append(db.OrCondition, map[string]interface{}{"query": query, "args": values})
	return db
}

func (db *gorpDB) IN(key string, value string) *gorpDB {

	in := key + " IN (" + value + ") "
	if db.Query != "" {

		db.Query = " AND " + in
	} else {

		db.Query = in
	}
	return db
}

func (db *gorpDB) GroupBy(value string) *gorpDB {

	db.group = " group by " + value
	return db
}
func (db *gorpDB) buildSql() string {

	sql := bytes.Buffer{}

	if len(db.Condition) > 0 {

		sql.WriteString(" WHERE ")

		i := 0
		for _, clause := range db.Condition {

			query := clause["query"].(string)
			values := clause["args"].([]interface{})
			if i > 0 {
				sql.WriteString(" AND ")
			}

			sql.WriteString(query)

			for _, vv := range values {

				db.params = append(db.params, vv)
			}
			i++
		}

	}
	if len(db.OrCondition) > 0 {

		sql.WriteString(" OR ")

		i := 0

		for _, clause := range db.OrCondition {

			query := clause["query"].(string)
			values := clause["args"].([]interface{})
			if i > 0 {
				sql.WriteString(" OR ")
			}

			sql.WriteString(query)

			for _, vv := range values {

				db.params = append(db.params, vv)
			}
			i++
		}

	}
	if db.inCondition != "" {

		if len(db.Condition) > 0 {

			sql.WriteString(" AND ")
		} else {
			sql.WriteString("  ")
		}
		sql.WriteString(db.inCondition)

	}
	return sql.String()
}
func (db *gorpDB) BuildSql() string {

	sql := bytes.Buffer{}
	if db.Query != "" {

		sql.WriteString(" WHERE ")
		sql.WriteString(db.Query)

	}

	if len(db.Condition) > 0 {

		if db.Query != "" {

			sql.WriteString(" AND ")
		} else {

			sql.WriteString(" WHERE ")
		}

		sql.WriteString(buildCondition(db.Condition))

	}
	if len(db.OrCondition) > 0 {

		sql.WriteString(" OR ")

		sql.WriteString(buildOrCondition(db.Condition))

	}
	return sql.String()
}
func buildCondition(w []map[string]interface{}) string {

	buff := bytes.NewBuffer([]byte{})
	i := 0

	for _, clause := range w {
		if sql := buildSelectQuery(clause); sql != "" {

			fmt.Println(sql)
			if i > 0 {
				buff.WriteString(" AND ")
			}
			buff.WriteString(sql)
			i++
		}

	}
	return buff.String()
}

func buildOrCondition(w []map[string]interface{}) string {

	buff := bytes.NewBuffer([]byte{})
	i := 0

	for _, clause := range w {
		if sql := buildSelectQuery(clause); sql != "" {

			fmt.Println(sql)
			if i > 0 {
				buff.WriteString(" Or ")
			}
			buff.WriteString(sql)
			i++
		}

	}
	return buff.String()
}

func buildSelectQuery(clause map[string]interface{}) (str string) {
	switch value := clause["query"].(type) {
	case string:
		str = value
	case []string:
		str = strings.Join(value, ", ")
	}

	args := clause["args"].([]interface{})

	buff := bytes.NewBuffer([]byte{})
	i := 0
	for pos, char := range str {
		if str[pos] == '?' {

			if m_type(args[i]) == "string" {
				buff.WriteString("'")
				buff.WriteString(args[i].(string))
				buff.WriteString("'")
			} else {
				buff.WriteString(fmt.Sprintf("%v", args[i]))
			}
			i++
		} else {
			buff.WriteRune(char)
		}
	}

	str = buff.String()

	return
}

func (db *gorpDB) Count(agrs ...interface{}) int32 {

	if db.table == "" {
		if len(agrs) == 0 {
			return 0
		}
		db.table = db.dbmap.GetTableName(agrs[0])
	}

	sql := bytes.Buffer{}
	sql.WriteString("SELECT count(")
	sql.WriteString(db.field)
	sql.WriteString(") FROM ")
	sql.WriteString(db.table)

	sql.WriteString(db.BuildSql())

	if db.group != "" {

		sql.WriteString(db.group)
	}

	var count int64 = 0
	count, db.Err = db.dbmap.SelectInt(sql.String())

	return int32(count)

}
func (db *gorpDB) Find(out interface{}) *gorpDB {

	if db.table == "" {

		db.table = db.dbmap.GetTableName(out)
	}

	sql := bytes.Buffer{}
	sql.WriteString("SELECT ")
	sql.WriteString(db.field)
	sql.WriteString(" FROM ")
	sql.WriteString(db.table)

	sql.WriteString(db.BuildSql())

	if db.group != "" {

		sql.WriteString(db.group)
	}
	if db.sort != "" {

		sql.WriteString(db.sort)
	}

	if db.Limit > 0 {

		ls := fmt.Sprintf(" limit %d,%d", db.Offset, db.Limit)
		sql.WriteString(ls)
	}

	_, db.Err = db.dbmap.Select(out, sql.String())
	return db
}
func (db *gorpDB) QueryField(field string, out interface{}) error {

	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT ")
	db_sql.WriteString(field)
	db_sql.WriteString(" FROM ")
	db_sql.WriteString(db.table)

	db_sql.WriteString(db.BuildSql())

	rows, err := db.dbmap.Db.Query(db_sql.String())
	if err != nil {

		//fmt.Errorf("gorp: cannot SELECT into this type: %v", err)
		return err
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	defer rows.Close()

	return rows.Scan(out)
}
func (db *gorpDB) FindById(out, id interface{}) error {

	if db.table == "" {

		db.table = db.dbmap.GetTableName(out)
	}

	sql := bytes.Buffer{}
	sql.WriteString("SELECT ")
	sql.WriteString(db.field)
	sql.WriteString(" FROM ")
	sql.WriteString(db.table)
	sql.WriteString(" WHERE id=?")

	return db.dbmap.SelectOne(out, sql.String(), id)
}
func (db *gorpDB) Get(out interface{}) error {

	if db.table == "" {

		db.table = db.dbmap.GetTableName(out)
	}

	sql := bytes.Buffer{}
	sql.WriteString("SELECT ")
	sql.WriteString(db.field)
	sql.WriteString(" FROM ")
	sql.WriteString(db.table)

	sql.WriteString(db.BuildSql())

	if db.group != "" {

		sql.WriteString(db.group)
	}

	return db.dbmap.SelectOne(out, sql.String())

}

func (db *gorpDB) IsExit() (bool, error) {

	if db.table == "" {
		return false, errors.New("no found model")
	}

	var out int64

	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT 1  FROM ")
	db_sql.WriteString(db.table)

	db_sql.WriteString(db.buildSql())
	db_sql.WriteString(" LIMIT 1")

	db.Err = db.dbmap.QueryRow(db_sql.String(), db.params...).Scan(&out)

	if db.Err != nil && db.Err.Error() == "sql: no rows in result set" {

		return false, nil
	}
	return out > 0, db.Err

}

func (db *gorpDB) First(out interface{}) error {

	if db.table == "" {

		db.table = db.dbmap.GetTableName(out)
	}

	sql := bytes.Buffer{}
	sql.WriteString("SELECT ")
	sql.WriteString(db.field)
	sql.WriteString(" FROM ")
	sql.WriteString(db.table)

	sql.WriteString(db.BuildSql())

	if db.group != "" {

		sql.WriteString(db.group)
	}

	sql.WriteString(" limit 1")

	return db.dbmap.SelectOne(out, sql.String())

}

func (db *gorpDB) Maps(maps map[string]interface{}) *gorpDB {

	i := 0
	s := bytes.Buffer{}
	if maps != nil && len(maps) > 0 {

		for k, v := range maps {

			if m_type(v) == "string" && v == "" {

				continue
			}
			if i > 0 {

				s.WriteString(" AND ")
			}
			if m_type(v) == "string" {

				s.WriteString(fmt.Sprintf(" %s='%s' ", k, v))
			} else {

				s.WriteString(fmt.Sprintf(" %s=%v ", k, v))
			}
			i++
		}
	}

	db.Query = s.String()

	return db
}
