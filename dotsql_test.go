package sqlmore

import (
	"bufio"
	"reflect"
	"strings"
	"testing"
)

type mss = map[string]string

func TestGetTag(t *testing.T) {
	var tests = []struct {
		line, want string
		attrs      mss
	}{
		{"SELECT 1+1", "", nil},
		{"-- Some Comment", "", mss{"Some": "", "Comment": ""}},
		{"-- name:  ", "", mss{"name": ""}},
		{"-- name: find-users-by-name dbtype: mysql", "find-users-by-name",
			mss{"name": "find-users-by-name", "dbtype": "mysql"}},
		{"  --  name:  save-user ", "save-user", mss{"name": "save-user"}},
	}

	for _, c := range tests {
		attrs, name := ParseDotTag(c.line, "--", "name")
		if name != c.want {
			t.Errorf("isTag('%s') == %s, expect %s", c.line, name, c.want)
		}

		if !reflect.DeepEqual(attrs, c.attrs) {
			t.Errorf("attrsOfTag('%s') == %v, expect %v", c.line, attrs, c.attrs)
		}
	}
}

func TestScannerRun(t *testing.T) {
	sqlFile := `
	-- name: all-users
	-- Finds all users
	SELECT * from USER
	-- name: empty-query-should-not-be-stored
	-- name: save-user
	INSERT INTO users (?, ?, ?)
	`

	scanner := &DotSQLScanner{}
	queries := scanner.Run(bufio.NewScanner(strings.NewReader(sqlFile)))

	numberOfQueries := len(queries)
	expectedQueries := 2

	if numberOfQueries != expectedQueries {
		t.Errorf("Scanner/Run() has %d queries instead of %d",
			numberOfQueries, expectedQueries)
	}
}
