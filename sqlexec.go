package sqlx

// refer https://yougg.github.io/2017/08/24/用go语言写一个简单的mysql客户端/
import (
	"database/sql"
	"strings"
	"time"
)

// ExecResult defines the result structure of sql execution.
type ExecResult struct {
	Error        error
	CostTime     time.Duration
	Headers      []string
	Rows         [][]string
	RowsAffected int64
	LastInsertID int64
	IsQuerySQL   bool
	FirstKey     string
}

// SQLExec wraps Exec method.
type SQLExec interface {
	// Exec execute query.
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

type ExecOption struct {
	MaxRows     int
	NullReplace string
	BlobReplace string
}

func (o ExecOption) reachMaxRows(row int) bool {
	return o.MaxRows > 0 && row >= o.MaxRows
}

// ExecSQL executes a SQL.
func ExecSQL(db SQLExec, sqlStr string, option ExecOption) ExecResult {
	firstKey, isQuerySQL := IsQuerySQL(sqlStr)

	if isQuerySQL {
		return processQuery(db, sqlStr, firstKey, option)
	}

	return execNonQuery(db, sqlStr, firstKey)
}

func processQuery(db SQLExec, sqlStr string, firstKey string, option ExecOption) ExecResult {
	start := time.Now()

	rows, err := db.Query(sqlStr)
	if err != nil || rows != nil && rows.Err() != nil {
		if err == nil {
			err = rows.Err()
		}

		return ExecResult{Error: err, CostTime: time.Since(start), IsQuerySQL: true, FirstKey: firstKey}
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return ExecResult{Error: err, CostTime: time.Since(start), IsQuerySQL: true, FirstKey: firstKey}
	}

	columnSize := len(columns)
	columnTypes, _ := rows.ColumnTypes()
	data := make([][]string, 0)

	var columnLobs []bool
	if option.BlobReplace != "" {
		columnLobs = make([]bool, columnSize)
		for i := 0; i < len(columnTypes); i++ {
			columnLobs[i] = ContainsFold(columnTypes[i].DatabaseTypeName(), "LOB")
		}
	}

	for row := 0; rows.Next() && !option.reachMaxRows(row); row++ {
		holders := make([]sql.NullString, columnSize)
		pointers := make([]interface{}, columnSize)

		for i := 0; i < columnSize; i++ {
			pointers[i] = &holders[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return ExecResult{Error: err, CostTime: time.Since(start), Headers: columns, Rows: data, IsQuerySQL: true}
		}

		values := make([]string, columnSize)

		for i, v := range holders {
			values[i] = IfElse(v.Valid, v.String, option.NullReplace)

			if option.BlobReplace != "" && v.Valid && columnLobs[i] {
				values[i] = "(" + columnTypes[i].DatabaseTypeName() + ")"
			}
		}

		data = append(data, values)
	}

	return ExecResult{Error: err, CostTime: time.Since(start), Headers: columns, Rows: data, IsQuerySQL: true}
}

func execNonQuery(db SQLExec, sqlStr string, firstKey string) ExecResult {
	start := time.Now()

	r, err := db.Exec(sqlStr)

	var affected int64

	if r != nil {
		affected, _ = r.RowsAffected()
	}

	var lastInsertID int64
	if r != nil && firstKey == "INSERT" {
		lastInsertID, _ = r.LastInsertId()
	}

	return ExecResult{Error: err,
		CostTime:     time.Since(start),
		RowsAffected: affected,
		IsQuerySQL:   false,
		LastInsertID: lastInsertID,
		FirstKey:     firstKey,
	}
}

// IsQuerySQL tests a sql is a query or not.
func IsQuerySQL(sql string) (string, bool) {
	key := FirstWord(sql)

	switch strings.ToUpper(key) {
	case "SELECT", "SHOW", "DESC", "DESCRIBE", "EXPLAIN":
		return key, true
	default: // "INSERT", "DELETE", "UPDATE", "SET", "REPLACE":
		return key, false
	}
}

// FirstWord returns the first word of the SQL statement s.
func FirstWord(s string) string {
	if fields := strings.Fields(strings.TrimSpace(s)); len(fields) > 0 {
		return fields[0]
	}

	return ""
}

// IfElse if else ...
func IfElse(ifCondition bool, ifValue, elseValue string) string {
	if ifCondition {
		return ifValue
	}

	return elseValue
}

// ContainsFold tell if a contains b in case-insensitively.
func ContainsFold(a, b string) bool {
	return strings.Contains(strings.ToUpper(a), strings.ToUpper(b))
}
