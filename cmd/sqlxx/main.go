package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/bingoohuang/sqlx"
	"github.com/bingoohuang/strcase"
	"github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

// nolint lll
type opts struct {
	DataSource string `short:"d" required:"true" long:"datasource" description:"datasource, eg. 127.0.0.1:9633 root/8BE4 db=test"`
}

// Table ...
type Table struct {
	Name    string `name:"TABLE_NAME"`
	Comment string `name:"TABLE_COMMENT"`
}

// Column ...
type Column struct {
	TableName  string `name:"TABLE_NAME"`
	ColumnName string `name:"COLUMN_NAME"`
	Comment    string `name:"COLUMN_COMMENT"`
	Type       string `name:"COLUMN_TYPE"` // eg. varchar(32)
	DataType   string `name:"DATA_TYPE"`   // eg. varchar
}

// nolint lll
type mysqlSchemaDao struct {
	Logger sqlx.DaoLogger

	Schema  func() string           `sql:"select database()"`
	Tables  func(s string) []Table  `sql:"select * from information_schema.TABLES where TABLE_SCHEMA=:1"`
	Columns func(s string) []Column `sql:"select * from information_schema.COLUMNS where TABLE_SCHEMA=:1 order by TABLE_NAME, ORDINAL_POSITION"`
}

func parseArgs() *opts {
	var opt opts

	if _, err := flags.ParseArgs(&opt, os.Args); err != nil {
		if ourErr, ok := err.(*flags.Error); ok && ourErr.Type != flags.ErrHelp {
			os.Exit(0)
		}

		fmt.Fprintf(os.Stderr, "database required set in the datasource flags\n")

		os.Exit(1)
	}

	return &opt
}

func main() {
	opt := parseArgs()
	ds := sqlx.CompatibleMySQLDs(opt.DataSource)
	db := sqlx.NewSQLMore("mysql", ds).Open()

	defer db.Close()

	dao := &mysqlSchemaDao{Logger: &sqlx.DaoLogrus{}}

	logrus.SetLevel(logrus.DebugLevel)

	if err := sqlx.CreateDao(db, dao); err != nil {
		panic(err)
	}

	schema := dao.Schema()
	if schema == "" {
		fmt.Fprintf(os.Stderr, "database required set in the datasource flags")

		os.Exit(1)
	}

	tables := dao.Tables(schema)
	tablesMap := make(map[string]Table)

	for _, t := range tables {
		tablesMap[t.Name] = t
	}

	columns := dao.Columns(schema)
	pkg := strings.ToLower(schema)
	_ = os.MkdirAll(pkg, 0755)

	gen(columns, tablesMap, pkg)
}

func gen(columns []Column, tablesMap map[string]Table, pkg string) {
	table := ""

	var (
		err     error
		daoFile *os.File
		dg      *daoGenerator
	)

	for _, c := range columns {
		if table != c.TableName {
			if daoFile != nil && dg != nil {
				dg.complete()
				dg.gen(daoFile)
				_ = daoFile.Close()
			}

			daoFile, err = os.Create(filepath.Join(pkg, strcase.ToSnake(c.TableName)+".go"))
			if err != nil {
				fmt.Fprintf(os.Stderr, "open file failed error %v\n", err)

				os.Exit(1)
			}

			table = c.TableName
			dg = newDaoGenerator(tablesMap[table], pkg)
		}

		if dg != nil {
			dg.addColumn(c)
		}
	}

	if daoFile != nil && dg != nil {
		dg.complete()
		dg.gen(daoFile)
		_ = daoFile.Close()
	}
}

func newDaoGenerator(table Table, pkg string) *daoGenerator {
	return &daoGenerator{
		table:         table,
		pkg:           pkg,
		structColumns: make([]string, 0),
		columns:       make([]Column, 0),
		imports:       make(map[string]bool),
	}
}

type daoGenerator struct {
	table         Table
	pkg           string
	structColumns []string
	columns       []Column
	imports       map[string]bool
}

func (g *daoGenerator) complete() {

}

var re = regexp.MustCompile(`\r?\n`) // nolint gochecknoglobals

func oneline(s string) string {
	return strings.TrimSpace(re.ReplaceAllString(s, " "))
}

func (g *daoGenerator) gen(w io.Writer) {
	var b bytes.Buffer

	g.writePackage(b)
	g.writeImports(b)
	g.writeStruct(b)

	_, _ = b.WriteTo(w)
}

func (g *daoGenerator) writeStruct(b bytes.Buffer) {
	structName := strcase.ToCamel(g.table.Name)
	b.WriteString("// " + structName + " represents table " + g.table.Name + ".\n")

	if tc := oneline(g.table.Comment); tc != "" {
		b.WriteString("// " + tc + "\n")
	}

	b.WriteString("type " + structName + " struct {\n")

	for _, c := range g.structColumns {
		b.WriteString(c)
		b.WriteString("\n")
	}

	b.WriteString("}\n")
}

func (g *daoGenerator) writeImports(b bytes.Buffer) {
	if len(g.imports) == 0 {
		return
	}

	importPkgs := make([]string, 0, len(g.imports))

	for k := range g.imports {
		importPkgs = append(importPkgs, k)
	}

	sort.Strings(importPkgs)

	b.WriteString("import (\n")

	for _, p := range importPkgs {
		b.WriteString("\t \"" + p + "\"\n")
	}

	b.WriteString(")\n")
}

func (g *daoGenerator) writePackage(b bytes.Buffer) {
	b.WriteString("package " + g.pkg + "\n\n")
}

func (g *daoGenerator) addColumn(c Column) {
	g.columns = append(g.columns, c)

	var b bytes.Buffer

	colGoType := columnGoType(c)

	pkg := ""
	if p := strings.LastIndex(colGoType, "."); p > 0 {
		pkg = colGoType[:p]
	}

	if pkg != "" {
		g.imports[pkg] = true
	}

	b.WriteString("\t" + strcase.ToCamel(c.ColumnName) +
		" " + colGoType + " `name:\"" + c.ColumnName + "\" `")

	if fc := oneline(c.Comment); fc != "" {
		b.WriteString("// " + fc)
	}

	g.structColumns = append(g.structColumns, b.String())
}

// nolint gomnd
func columnGoType(c Column) string {
	typ := strings.ToLower(c.DataType)
	switch typ {
	case "tinyint", "smallint", "mediumint", "int", "integer":
		return "int"
	case "bigint":
		return "int64"
	case "float", "decimal":
		return "float32"
	case "double":
		return "float64"

	case "char", "varchar",
		"tinyblob", "blob", "mediumblob", "longblob",
		"tinytext", "text", "mediumtext", "longtext":
		return "string"
	case "date", "datetime", "timestamp", "time":
		return "time.Time"
	default:
		return "string"
	}
}
