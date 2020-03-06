// nolint gomnd
package sqlx

import (
	"bufio"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
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

func TestLoad(t *testing.T) {
	_, err := DotSQLLoad(strings.NewReader(""))
	assert.Nil(t, err)
}

func TestLoadFromFile(t *testing.T) {
	dot, err := DotSQLLoadFile("./non-existent.sql")
	assert.NotNil(t, err, "error expected to be non-nil, got nil")
	assert.Nil(t, dot, "dotsql instance expected to be nil, got non-nil")

	dot, err = DotSQLLoadFile("testdata/test_schema.sql")
	assert.Nil(t, err)

	assert.NotNil(t, dot, "dotsql instance expected to be non-nil, got nil")
}

func TestLoadFromString(t *testing.T) {
	_, err := DotSQLLoadString("")
	assert.Nil(t, err)
}

func TestRaw(t *testing.T) {
	expectedQuery := "SELECT 1+1"

	dot, err := DotSQLLoadString("--name: my-query\n" + expectedQuery)
	assert.Nil(t, err)

	got, err := dot.Raw("my-query")
	assert.Nil(t, err)

	got = strings.TrimSpace(got)
	assert.Equal(t, expectedQuery, got, "Raw() == '%s', expected '%s'", got, expectedQuery)
}

func TestQueries(t *testing.T) {
	expectedQueryMap := map[string]string{
		"select": "SELECT * from users",
		"insert": "INSERT INTO users (?, ?, ?)",
	}

	dot, err := DotSQLLoadString(`
	-- name: select
	SELECT * from users;

	-- name: insert
	INSERT INTO users (?, ?, ?)
	`)
	assert.Nil(t, err)

	got := dot.Sqls

	if len(got) != len(expectedQueryMap) {
		t.Errorf("QueryMap() len (%d) differ from expected (%d)", len(got), len(expectedQueryMap))
	}

	for name, query := range got {
		if query.Content != expectedQueryMap[name] {
			t.Errorf("QueryMap()[%s] == '%s', expected '%s'", name, query, expectedQueryMap[name])
		}
	}
}

func TestParseSQL(t *testing.T) {
	parsed, err := parseSQL("", "insert into person(name, age) values(:name, :age)")
	assert.Nil(t, err)
	assert.Equal(t, &sqlParsed{
		SQL:    "insert into person(name, age) values(?, ?)",
		BindBy: byName,
		Vars:   []string{"name", "age"},
		MaxSeq: 2,
	}, parsed)

	parsed, err = parseSQL("", "insert into person(name, age) values(:1, :2)")
	assert.Nil(t, err)
	assert.Equal(t, &sqlParsed{
		SQL:    "insert into person(name, age) values(?, ?)",
		BindBy: bySeq,
		Vars:   []string{"1", "2"},
		MaxSeq: 2,
	}, parsed)

	parsed, err = parseSQL("", "insert into person(name, age) values(:, :)")
	assert.Nil(t, err)
	assert.Equal(t, &sqlParsed{
		SQL:    "insert into person(name, age) values(?, ?)",
		BindBy: byAuto,
		Vars:   []string{"", ""},
		MaxSeq: 2,
	}, parsed)

	parsed, err = parseSQL("", "insert into person(name, age) values('a', 'b')")
	assert.Nil(t, err)
	assert.Equal(t, &sqlParsed{
		SQL:    "insert into person(name, age) values('a', 'b')",
		BindBy: byNone,
		Vars:   []string{},
	}, parsed)

	parsed, err = parseSQL("", "insert into person(name, age) values(:, :age)")
	assert.Nil(t, parsed)
	assert.NotNil(t, err)
}
