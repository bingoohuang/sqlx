package sqlmore

// refer https://yougg.github.io/2017/08/24/用go语言写一个简单的mysql客户端/
import (
	"database/sql"
	"strings"
	"time"
)

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

func ExecSQL(db *sql.DB, sqlStr string, maxRows int, nullReplace string) ExecResult {
	start := time.Now()

	firstKey, isQuerySQL := IsQuerySQL(sqlStr)
	if !isQuerySQL {
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
			IsQuerySQL:   isQuerySQL,
			LastInsertID: lastInsertID,
			FirstKey:     firstKey,
		}
	}

	rows, err := db.Query(sqlStr)
	if err != nil {
		return ExecResult{Error: err, CostTime: time.Since(start), IsQuerySQL: isQuerySQL, FirstKey: firstKey}
	}

	columns, err := rows.Columns()
	if err != nil {
		return ExecResult{Error: err, CostTime: time.Since(start), IsQuerySQL: isQuerySQL, FirstKey: firstKey}
	}

	columnSize := len(columns)

	columnTypes, _ := rows.ColumnTypes()
	columnLobs := make([]bool, columnSize)
	for i := 0; i < len(columnTypes); i++ {
		columnLobs[i] = ContainsIgnoreCase(columnTypes[i].DatabaseTypeName(), "LOB")
	}

	data := make([][]string, 0)
	for row := 0; rows.Next() && (maxRows == 0 || row < maxRows); row++ {
		holders := make([]sql.NullString, columnSize)
		pointers := make([]interface{}, columnSize)
		for i := 0; i < columnSize; i++ {
			pointers[i] = &holders[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return ExecResult{Error: err, CostTime: time.Since(start), Headers: columns,
				Rows: data, IsQuerySQL: isQuerySQL}
		}

		values := make([]string, columnSize)
		for i, v := range holders {
			values[i] = IfElse(v.Valid, v.String, nullReplace)
			if columnLobs[i] && v.Valid {
				values[i] = "(" + columnTypes[i].DatabaseTypeName() + ")"
			}
		}

		data = append(data, values)
	}

	return ExecResult{Error: err, CostTime: time.Since(start), Headers: columns,
		Rows: data, IsQuerySQL: isQuerySQL}
}

func IsQuerySQL(sql string) (string, bool) {
	key := ""
	if fields := strings.Fields(strings.TrimSpace(sql)); len(fields) > 0 {
		key = strings.ToUpper(fields[0])
	}
	switch key {
	case "INSERT", "DELETE", "UPDATE", "SET":
		return key, false
	case "SELECT", "SHOW", "DESC", "EXPLAIN":
		return key, true
	default:
		return key, false
	}
}

func IfElse(ifCondition bool, ifValue, elseValue string) string {
	if ifCondition {
		return ifValue
	}
	return elseValue
}

func ContainsIgnoreCase(a, b string) bool {
	return strings.Contains(strings.ToUpper(a), strings.ToUpper(b))
}
