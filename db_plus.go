package gorp

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
)

var prefix string = "tb_"

type gorpDB struct {
	dbmap     *DbMap
	parent    *gorpDB
	tx        *Transaction
	Query     string
	Condition []map[string]interface{}
	table     string
	Cmd       string
	field     string
	Offset    int32
	Limit     int32
	sort      string
	Err       error
}

func (m *DbMap) SetPrefix(s string) {

	prefix = s
}

//ad dbMap new month
func (m *DbMap) Model(class interface{}) *gorpDB {

	db := &gorpDB{dbmap: m, parent: nil, tx: nil, Query: "", table: "", Condition: nil, field: "*", Cmd: "", Offset: 0, Limit: 0, sort: ""}

	db.table = db.GetTable(class)
	return db

}
func (m *DbMap) Table(name string) *gorpDB {
	
	db := &gorpDB{dbmap: m, parent: nil, tx: nil, Query: "", table: "", Condition: nil, field: "*", Cmd: "", Offset: 0, Limit: 0, sort: ""}

	db.table = name
	return db
}
func (m *DbMap) Where(query string, values ...interface{}) *gorpDB {

	db := &gorpDB{dbmap: m, parent: nil, tx: nil, Query: "", table: "", Condition: nil, field: "*", Cmd: "", Offset: 0, Limit: 0, sort: ""}

	db.Condition = append(db.Condition, map[string]interface{}{"query": query, "args": values})
	return db
}
func (m *DbMap) Tx(tx *Transaction) *gorpDB {

	db := &gorpDB{dbmap: m, parent: nil, tx: nil, Query: "", table: "", Condition: nil, field: "*", Cmd: "", Offset: 0, Limit: 0, sort: ""}
	db.tx = tx
	return db
}

func (m *DbMap) FindById(out, id interface{}) error {

	_, err := m.Get(out, id)
	return err
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
	default:
		return ""
	}

}
func (s *gorpDB) GetTable(class interface{}) string {

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

/*
func (db *gorpDB) Model(class interface{}) *gorpDB {

	db.table = db.GetTable(class)
	return db

}*/
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
		db.table = db.GetTable(class)
	}
	sql := bytes.Buffer{}

	sql.WriteString("DELETE  FROM ")
	sql.WriteString(db.table)

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

func (db *gorpDB) Count(class interface{}) int32 {

	if db.table == "" {

		db.table = db.GetTable(class)
	}

	sql := bytes.Buffer{}
	sql.WriteString("SELECT count(")
	sql.WriteString(db.field)
	sql.WriteString(") FROM ")
	sql.WriteString(db.table)

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

	var count int64 = 0
	count, db.Err = db.dbmap.SelectInt(sql.String())

	return int32(count)

}
func (db *gorpDB) Find(out interface{}) *gorpDB {

	if db.table == "" {

		db.table = db.GetTable(out)
	}

	sql := bytes.Buffer{}
	sql.WriteString("SELECT ")
	sql.WriteString(db.field)
	sql.WriteString(" FROM ")
	sql.WriteString(db.table)

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

func (db *gorpDB) FindById(out, id interface{}) error {

	if db.table == "" {

		db.table = db.GetTable(out)
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

		db.table = db.GetTable(out)
	}

	sql := bytes.Buffer{}
	sql.WriteString("SELECT ")
	sql.WriteString(db.field)
	sql.WriteString(" FROM ")
	sql.WriteString(db.table)

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
