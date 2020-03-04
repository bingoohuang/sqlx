// nolint gomnd
package sqlmore

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/bingoohuang/strcase"
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
	SELECT * from users
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

type person struct {
	MyName string `name:"name"`
	Age    int
}

type personDao struct {
	CreatePersonTable func()                   `sql:"create table person(name varchar(100), age int)"`
	Insert            func(person)             `sql:"insert into person(name, age) values(:name, :age)"`
	Find              func(name string) person `sql:"select name, age from person where name = :1"`
	List              func() []person          `sql:"select name, age from person"`
	ListByName        func(string) []person    `sql:"select name, age from person where name=:"`
	Delete            func(string) int         `sql:"delete from person where name = :"`

	GetAge func(name string) struct{ Age int } `sql:"select age from person where name=:1"`
}

func TestDao(t *testing.T) {
	// 生成DAO
	dao := &personDao{}
	assert.Nil(t, CreateDao("sqlite3", openDB(t), dao))

	// 建表
	dao.CreatePersonTable()
	// 插入
	dao.Insert(person{MyName: "bingoohuang", Age: 100})
	// 查找
	assert.Equal(t, person{MyName: "bingoohuang", Age: 100}, dao.Find("bingoohuang"))
	// 刪除
	assert.Equal(t, 1, dao.Delete("bingoohuang"))
	// 再找，找不到，返回零值ß
	assert.Zero(t, dao.Find("bingoohuang"))
	// 插入
	dao.Insert(person{MyName: "dingoohuang", Age: 200})
	dao.Insert(person{MyName: "pingoohuang", Age: 300})
	// 列表
	assert.Equal(t, []person{{MyName: "dingoohuang", Age: 200}, {MyName: "pingoohuang", Age: 300}}, dao.List())
	// 条件列表
	assert.Equal(t, []person{{MyName: "dingoohuang", Age: 200}}, dao.ListByName("dingoohuang"))

	assert.Equal(t, struct{ Age int }{Age: 200}, dao.GetAge("dingoohuang"))
}

func openDB(t *testing.T) *sql.DB {
	// 创建数据库连接池
	db, err := sql.Open("sqlite3", ":memory:")
	assert.Nil(t, err)
	return db
}

func CreateDao(driverName string, db *sql.DB, dao interface{}) error {
	v := reflect.ValueOf(dao)
	v = reflect.Indirect(v)

	for i := 0; i < v.NumField(); i++ {
		fv := v.Field(i)
		f := v.Type().Field(i)

		if f.PkgPath != "" { // Is not exportable?
			continue
		}

		if f.Type.Kind() != reflect.Func {
			continue
		}

		sqlStmt := f.Tag.Get("sql")
		parsedSQL, err := parseSQL(sqlStmt, "?")
		if err != nil {
			return fmt.Errorf("failed to parse sql %v error %v", sqlStmt, err)
		}

		numIn := f.Type.NumIn()
		numOut := f.Type.NumOut()

		if numIn == 0 && !parsedSQL.IsBindBy(ByNone) {
			return fmt.Errorf("sql %s required bind varialbes, but the func %v has none", sqlStmt, f.Type)
		}

		if numIn != 1 && parsedSQL.IsBindBy(ByName) {
			return fmt.Errorf("sql %s required named varialbes, but the func %v has non-one arguments",
				sqlStmt, f.Type)
		}

		if parsedSQL.IsBindBy(BySeq, ByAuto) {
			if numIn < parsedSQL.MaxSeq {
				return fmt.Errorf("sql %s required max %d vars, but the func %v has only %d arguments",
					sqlStmt, parsedSQL.MaxSeq, f.Type, numIn)
			}
		}

		_, isQuerySQL := IsQuerySQL(sqlStmt)

		fn := reflect.MakeFunc(f.Type, func(args []reflect.Value) (results []reflect.Value) {
			switch {
			case numIn == 0 && numOut == 0:
				return executeRaw(db, parsedSQL.ParsedSQL)
			case numIn == 1 && parsedSQL.IsBindBy(ByName) && numOut == 0:
				return executeRawByNamedArgs(db, parsedSQL, args[0])
			case isQuerySQL && parsedSQL.IsBindBy(BySeq, ByAuto, ByNone) && numOut == 1:
				return executeQueryBySeqAndReturnOne(db, parsedSQL, f.Type.Out(0), args)
			case !isQuerySQL && parsedSQL.IsBindBy(BySeq, ByAuto) && numOut == 1:
				return executeRawBySeqAndReturnOne(db, parsedSQL, f.Type.Out(0), args)
			}

			return []reflect.Value{}
		})

		fv.Set(fn)
	}

	return nil
}

type BindBy int

const (
	ByNone BindBy = iota
	ByAuto
	BySeq
	ByName
)

func (b BindBy) String() string {
	switch b {
	case ByNone:
		return "ByNone"
	case ByAuto:
		return "ByAuto"
	case BySeq:
		return "BySeq"
	case ByName:
		return "ByName"
	}

	return "Unknown"
}

type SQLParsed struct {
	ParsedSQL string
	BindBy    BindBy
	Vars      []string
	MaxSeq    int
}

func (p SQLParsed) IsBindBy(by ...BindBy) bool {
	for _, b := range by {
		if p.BindBy == b {
			return true
		}
	}

	return false
}

var sqlre = regexp.MustCompile(`:\w*`) // nolint gochecknoglobals

func parseSQL(stmt, bindMark string) (*SQLParsed, error) {
	vars := make([]string, 0)
	parsed := sqlre.ReplaceAllStringFunc(stmt, func(bindVar string) string {
		vars = append(vars, bindVar[1:])
		return bindMark
	})

	bindBy, maxSeq, err := parseBindBy(vars)
	if err != nil {
		return nil, err
	}

	return &SQLParsed{
		ParsedSQL: parsed,
		BindBy:    bindBy,
		Vars:      vars,
		MaxSeq:    maxSeq,
	}, nil
}

func parseBindBy(vars []string) (BindBy, int, error) {
	bindBy := ByNone
	maxSeq := 0

	for _, v := range vars {
		if v == "" {
			if bindBy == ByAuto {
				maxSeq++
				continue
			}

			if bindBy != ByNone {
				return 0, 0, fmt.Errorf("mixed bind mod found (%v-%v)", bindBy, ByAuto)
			}

			bindBy = ByAuto
			maxSeq++

			continue
		}

		n, err := strconv.Atoi(v)
		if err == nil {
			if bindBy == BySeq {
				if maxSeq < n {
					maxSeq = n
				}

				continue
			}

			if bindBy != ByNone {
				return 0, 0, fmt.Errorf("mixed bind mod found (%v-%v)", bindBy, BySeq)
			}

			bindBy = BySeq
			maxSeq = n

			continue
		}

		if bindBy == ByName {
			maxSeq++
			continue
		}

		if bindBy != ByNone {
			return 0, 0, fmt.Errorf("mixed bind mod found (%v-%v)", bindBy, ByName)
		}

		bindBy = ByName
		maxSeq++
	}

	return bindBy, maxSeq, nil
}

func TestParseSQL(t *testing.T) {
	parsed, err := parseSQL("insert into person(name, age) values(:name, :age)", "?")
	assert.Nil(t, err)
	assert.Equal(t, &SQLParsed{
		ParsedSQL: "insert into person(name, age) values(?, ?)",
		BindBy:    ByName,
		Vars:      []string{"name", "age"},
		MaxSeq:    2,
	}, parsed)

	parsed, err = parseSQL("insert into person(name, age) values(:1, :2)", "?")
	assert.Nil(t, err)
	assert.Equal(t, &SQLParsed{
		ParsedSQL: "insert into person(name, age) values(?, ?)",
		BindBy:    BySeq,
		Vars:      []string{"1", "2"},
		MaxSeq:    2,
	}, parsed)

	parsed, err = parseSQL("insert into person(name, age) values(:, :)", "?")
	assert.Nil(t, err)
	assert.Equal(t, &SQLParsed{
		ParsedSQL: "insert into person(name, age) values(?, ?)",
		BindBy:    ByAuto,
		Vars:      []string{"", ""},
		MaxSeq:    2,
	}, parsed)

	parsed, err = parseSQL("insert into person(name, age) values('a', 'b')", "?")
	assert.Nil(t, err)
	assert.Equal(t, &SQLParsed{
		ParsedSQL: "insert into person(name, age) values('a', 'b')",
		BindBy:    ByNone,
		Vars:      []string{},
	}, parsed)

	parsed, err = parseSQL("insert into person(name, age) values(:, :age)", "?")
	assert.Nil(t, parsed)
	assert.NotNil(t, err)
}

func executeRaw(db *sql.DB, stmt string) []reflect.Value {
	fmt.Printf("start to execute %s\n", stmt)
	_, err := db.Exec(stmt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "execute %s error %v\n", stmt, err)
	}

	return []reflect.Value{}
}

func executeRawByNamedArgs(db *sql.DB, parsedSQL *SQLParsed, bean reflect.Value) []reflect.Value {
	vars := make([]interface{}, len(parsedSQL.Vars))

	for i, name := range parsedSQL.Vars {
		fv := bean.FieldByNameFunc(func(field string) bool {
			f, _ := bean.Type().FieldByName(field)
			tagName := f.Tag.Get("name")
			if tagName != "" {
				return tagName == name
			}

			return strings.EqualFold(field, name) || strings.EqualFold(field, strcase.ToCamel(name))
		})

		vars[i] = fv.Interface()
	}

	stmt := parsedSQL.ParsedSQL
	fmt.Printf("start to execute %s with args %v\n", stmt, vars)
	_, err := db.Exec(stmt, vars...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "execute %s error %v\n", stmt, err)
	}

	return []reflect.Value{}
}

func executeRawBySeqAndReturnOne(db *sql.DB, parsedSQL *SQLParsed, outType reflect.Type, args []reflect.Value) []reflect.Value {
	vars := makeVars(parsedSQL, args)
	stmt := parsedSQL.ParsedSQL
	fmt.Printf("start to execute %s with args %v\n", stmt, vars)
	result, err := db.Exec(stmt, vars...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "execute %s error %v\n", stmt, err)

		return []reflect.Value{reflect.ValueOf(0)}
	}

	affected, err := convertRowsAffected(result, stmt, outType)
	if err != nil {
		fmt.Fprintf(os.Stderr, "execute %s error %v\n", stmt, err)

		return []reflect.Value{reflect.ValueOf(0)}
	}

	return []reflect.Value{affected}
}

func executeQueryBySeqAndReturnOne(db *sql.DB, parsedSQL *SQLParsed, outType reflect.Type, args []reflect.Value) []reflect.Value {
	vars := makeVars(parsedSQL, args)
	isOutSlice := outType.Kind() == reflect.Slice
	outSlice := reflect.Value{}

	if isOutSlice {
		outSlice = reflect.MakeSlice(outType, 0, 0)
		outType = outType.Elem()
	}

	out := reflect.Indirect(reflect.New(outType))

	stmt := parsedSQL.ParsedSQL
	fmt.Printf("start to execute %s with args %v\n", stmt, vars)
	rows, err := db.Query(stmt, vars...)
	if err != nil || rows.Err() != nil {
		if err == nil {
			err = rows.Err()
		}

		fmt.Fprintf(os.Stderr, "execute %s error %v\n", stmt, err)

		return []reflect.Value{out}
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(os.Stderr, "get columns %s error %v\n", stmt, err)

		return []reflect.Value{out}
	}

	columnSize := len(columns)
	pointers := make([]interface{}, columnSize)
	mapFields := make([]*reflect.StructField, columnSize)

	for i, col := range columns {
		fv, ok := outType.FieldByNameFunc(func(field string) bool {
			f, _ := outType.FieldByName(field)
			tagName := f.Tag.Get("name")
			if tagName != "" {
				return tagName == col
			}

			return strings.EqualFold(field, col) || strings.EqualFold(field, strcase.ToCamel(col))
		})

		if ok {
			pointers[i] = reflect.New(fv.Type).Interface()
			mapFields[i] = &fv
		} else {
			pointers[i] = &sql.NullString{}
		}
	}

	for ri := 0; rows.Next(); ri++ {
		if ri > 0 {
			out = reflect.Indirect(reflect.New(outType))

			for i := range columns {
				if mapFields[i] != nil {
					pointers[i] = reflect.New(mapFields[i].Type).Interface()
				}
			}
		}

		if err := rows.Scan(pointers...); err != nil {
			fmt.Fprintf(os.Stderr, "scan rows %s error %v\n", stmt, err)

			return []reflect.Value{out}
		}

		for i, field := range mapFields {
			if field == nil {
				continue
			}

			f := out.FieldByName(field.Name)
			f.Set(reflect.Indirect(reflect.ValueOf(pointers[i])))
		}

		if isOutSlice {
			outSlice = reflect.Append(outSlice, out)
		}

	}

	if isOutSlice {
		return []reflect.Value{outSlice}
	}

	return []reflect.Value{out}
}

func makeVars(parsedSQL *SQLParsed, args []reflect.Value) []interface{} {
	vars := make([]interface{}, 0, len(parsedSQL.Vars))

	for i, name := range parsedSQL.Vars {
		if parsedSQL.BindBy == ByAuto {
			vars = append(vars, args[i].Interface())
		} else {
			seq, _ := strconv.Atoi(name)
			vars = append(vars, args[seq-1].Interface())
		}
	}
	return vars
}

func convertRowsAffected(result sql.Result, stmt string, outType reflect.Type) (reflect.Value, error) {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return reflect.Value{}, fmt.Errorf("rowsAffected %s error %w", stmt, err)
	}

	rowsAffectedV := reflect.ValueOf(rowsAffected)
	if rowsAffectedV.Type().ConvertibleTo(outType) {
		return rowsAffectedV.Convert(outType), nil
	}

	return reflect.Value{}, fmt.Errorf("unable to convert %v to type %v", rowsAffected, outType)
}
