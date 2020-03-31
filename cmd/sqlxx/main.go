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

	"github.com/bingoohuang/gou/str"

	"github.com/bingoohuang/sqlx"
	"github.com/bingoohuang/strcase"
	flags "github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

// nolint lll
type opts struct {
	DataSource string `short:"d" required:"true" long:"dsn" description:"dsn, eg. root:8BE4@127.0.0.1:9633/test"`
	Pkg        string `short:"p" required:"false" long:"pkg" description:"package name, default lowercase of database name"`
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
	ColumnKey  string `name:"COLUMN_KEY"`  // eg. PRI, MUL, UNI
	Extra      string `name:"EXTRA"`       // eg. auto_increment
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
	db := sqlx.NewSQLMore("mysql", sqlx.CompatibleMySQLDs(opt.DataSource)).Open()

	defer db.Close()

	sqlx.DB = db

	logrus.SetLevel(logrus.DebugLevel)

	dao := &mysqlSchemaDao{Logger: &sqlx.DaoLogrus{}}
	if err := sqlx.CreateDao(dao); err != nil {
		panic(err)
	}

	schema := dao.Schema()
	if schema == "" {
		fmt.Fprintf(os.Stderr, "database required set in the dsn flags")

		os.Exit(1)
	}

	tablesMap := make(map[string]Table)
	for _, t := range dao.Tables(schema) {
		tablesMap[t.Name] = t
	}

	columns := dao.Columns(schema)

	pkg := str.EmptyThen(opt.Pkg, strings.ToLower(schema))
	_ = os.MkdirAll(pkg, 0750)

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
	table          Table
	pkg            string
	structColumns  []string
	columns        []Column
	imports        map[string]bool
	b              bytes.Buffer
	keyColumns     []Column
	noneKeyColumns []Column
	autoIncrement  bool
}

func (g *daoGenerator) complete() {

}

// nolint gochecknoglobals
var (
	re1 = regexp.MustCompile(`\r?\n`)
	re2 = regexp.MustCompile(`\s{2,}`)
)

func line(s string) string {
	s = re1.ReplaceAllString(s, " ")
	s = strings.TrimSpace(re2.ReplaceAllString(s, " "))

	return s
}
func (g *daoGenerator) gen(w io.Writer) {
	g.prepare()

	g.writePackage()
	g.writeImports()
	g.writeStruct()
	g.writeDAO()
	g.writeDAOCreator()
	g.writeSQL()

	_, _ = g.b.WriteTo(w)
}

func (g *daoGenerator) writeDAOCreator() {
	beanName := strcase.ToCamel(g.table.Name)
	daoName := beanName + "DAO"
	structName := "Create" + daoName + "E"
	g.b.WriteString("// " + structName + " represents DAO creator for table " + g.table.Name + ".\n")
	g.b.WriteString("func " + structName + " () (*" + daoName + ", error) {\n")
	g.b.WriteString("\tdao := &" + daoName + "{Logger: &sqlx.DaoLogrus{}}\n")
	g.b.WriteString("\n")
	// nolint lll
	g.b.WriteString("\tif err := sqlx.CreateDao(dao, sqlx.WithSQLStr(" + strcase.ToCamelLower(g.table.Name) + "SQL)); err != nil {\n")
	g.b.WriteString("\t\treturn nil, err;\n")
	g.b.WriteString("\t}\n")
	g.b.WriteString("\n")
	g.b.WriteString("\treturn dao, nil\n")
	g.b.WriteString("}\n\n")

	structName = "Create" + daoName
	g.b.WriteString("// " + structName + " represents DAO creator for table " + g.table.Name + ".\n")
	g.b.WriteString("func " + structName + " () *" + daoName + " {\n")
	g.b.WriteString("\tdao, err := " + structName + "E()\n")
	g.b.WriteString("\tif err != nil {\n")
	g.b.WriteString("\t\tpanic(err)\n")
	g.b.WriteString("\t}\n")
	g.b.WriteString("\n")
	g.b.WriteString("\treturn dao\n")
	g.b.WriteString("}\n\n")
}

func (g *daoGenerator) writeDAO() {
	beanName := strcase.ToCamel(g.table.Name)
	structName := beanName + "DAO"
	g.b.WriteString("// " + structName + " represents DAO operations for table " + g.table.Name + ".\n")

	g.b.WriteString("type " + structName + " struct {\n")

	if g.autoIncrement {
		g.b.WriteString("\tInsert func(" + beanName + ") (lastInsertID int64) `sqlName:\"Insert" + beanName + "\"`\n")
	} else {
		g.b.WriteString("\tInsert func(" + beanName + ")int `sqlName:\"Insert" + beanName + "\"`\n")
	}

	if len(g.keyColumns) == 1 {
		c := g.keyColumns[0]
		args := strcase.ToCamelLower(c.ColumnName) + " " + columnGoType(c)
		g.b.WriteString("\tDelete func(" + args + ")(effectedRows int) `sqlName:\"Delete" + beanName + "\"`\n")
		g.b.WriteString("\tUpdate func(" + args + ")(effectedRows int) `sqlName:\"Update" + beanName + "\"`\n")
		g.b.WriteString("\tFind func(" + args + ")(" + beanName + ", error) `sqlName:\"Find" + beanName + "\"`\n")
	} else {
		g.b.WriteString("\tDelete func(" + beanName + ")(effectedRows int) `sqlName:\"Delete" + beanName + "\"`\n")
		g.b.WriteString("\tUpdate func(" + beanName + ")(effectedRows int) `sqlName:\"Update" + beanName + "\"`\n")
		g.b.WriteString("\tFind func(" + beanName + ")(" + beanName + ", error) `sqlName:\"Find" + beanName + "\"`\n")
	}

	g.b.WriteString("\tSelectAll func()[]" + beanName + " `sqlName:\"SelectAll" + beanName + "\"`\n")

	g.b.WriteString("\n")
	g.b.WriteString("\tLogger sqlx.DaoLogger\n")
	g.b.WriteString("\tErr error\n")
	g.b.WriteString("}\n\n")
}

func (g *daoGenerator) writeStruct() {
	structName := strcase.ToCamel(g.table.Name)
	g.b.WriteString("// " + structName + " represents a structure mapping for row of table " + g.table.Name + ".\n")

	if tc := line(g.table.Comment); tc != "" {
		g.b.WriteString("// " + tc + "\n")
	}

	g.b.WriteString("type " + structName + " struct {\n")

	for _, c := range g.structColumns {
		g.b.WriteString(c)
		g.b.WriteString("\n")
	}

	g.b.WriteString("}\n\n")
}

func (g *daoGenerator) writeImports() {
	g.b.WriteString("import (\n")
	g.b.WriteString("\t \"github.com/bingoohuang/sqlx\"\n")

	importPkgs := make([]string, 0, len(g.imports))
	for k := range g.imports {
		importPkgs = append(importPkgs, k)
	}

	sort.Strings(importPkgs)

	for _, p := range importPkgs {
		g.b.WriteString("\t \"" + p + "\"\n")
	}

	g.b.WriteString(")\n\n")
}

func (g *daoGenerator) writePackage() {
	g.b.WriteString("package " + g.pkg + "\n\n")
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

	if fc := line(c.Comment); fc != "" {
		b.WriteString("// " + fc)
	}

	g.structColumns = append(g.structColumns, b.String())
}

func (g *daoGenerator) writeSQL() {
	g.b.WriteString("const " + strcase.ToCamelLower(g.table.Name) + "SQL = `")

	g.writeSQLInsert()
	g.writeSQLDelete()
	g.writeSQLUpdate()
	g.writeSQLSelectAll()
	g.writeSQLFind()

	g.b.WriteString("`\n\n")
}

func (g *daoGenerator) writeSQLUpdate() {
	g.b.WriteString("\n-- name: Update" + strcase.ToCamel(g.table.Name) + "\nupdate " + g.table.Name + "\nset\n")

	for i, c := range g.noneKeyColumns {
		if i > 0 {
			g.b.WriteString(",")
		} else {
			g.b.WriteString(" ")
		}

		g.b.WriteString("    " + c.ColumnName + " = :" + c.ColumnName + "\n")
	}

	g.b.WriteString("where\n")

	g.genWhereColumns(false)

	g.b.WriteString(";\n")
}

func (g *daoGenerator) writeSQLDelete() {
	g.b.WriteString("\n-- name: Delete" + strcase.ToCamel(g.table.Name) + "\ndelete from " + g.table.Name + "\nwhere\n")

	g.genWhereColumns(true)

	g.b.WriteString(";\n")
}

func (g *daoGenerator) genWhereColumns(pos bool) {
	if len(g.keyColumns) == 1 && pos {
		g.b.WriteString(g.keyColumns[0].ColumnName + "= ':1'")
		return
	}

	for i, c := range g.whereColumns() {
		if i > 0 {
			g.b.WriteString(",")
		} else {
			g.b.WriteString(" ")
		}

		g.b.WriteString("    " + c.ColumnName + " = :" + c.ColumnName + "\n")
	}
}

func (g *daoGenerator) whereColumns() []Column {
	if len(g.keyColumns) > 0 {
		return g.keyColumns
	}

	return g.columns
}
func (g *daoGenerator) writeSQLInsert() {
	g.b.WriteString("\n-- name: Insert" + strcase.ToCamel(g.table.Name) + "\ninsert into " + g.table.Name + "\n")

	for i, c := range g.columns {
		if i == 0 {
			g.b.WriteString("(")
		} else {
			g.b.WriteString(", ")
		}

		g.b.WriteString(c.ColumnName)
	}

	g.b.WriteString(")\n")
	g.b.WriteString("values\n")

	for i, c := range g.columns {
		if i == 0 {
			g.b.WriteString("(")
		} else {
			g.b.WriteString(", ")
		}

		g.b.WriteString(":" + c.ColumnName)
	}

	g.b.WriteString(");\n")
}

func (g *daoGenerator) writeSQLSelectAll() {
	g.b.WriteString("\n-- name: SelectAll" + strcase.ToCamel(g.table.Name) + "\nselect ")

	for i, c := range g.columns {
		if i > 0 {
			g.b.WriteString(", ")
		}

		g.b.WriteString(c.ColumnName)
	}

	g.b.WriteString("\nfrom " + g.table.Name)
	g.b.WriteString(";\n")
}

func (g *daoGenerator) writeSQLFind() {
	if len(g.keyColumns) == 0 {
		return
	}

	g.b.WriteString("\n-- name: Find" + strcase.ToCamel(g.table.Name) + "\nselect ")

	for i, c := range g.columns {
		if i > 0 {
			g.b.WriteString(", ")
		}

		g.b.WriteString(c.ColumnName)
	}

	g.b.WriteString("\nfrom " + g.table.Name + "\nwhere \n")
	g.genWhereColumns(true)
	g.b.WriteString(";\n")
}

func (g *daoGenerator) prepare() {
	g.prepareKeyColumns()
	g.findAutoIncrement()
}

func (g *daoGenerator) prepareKeyColumns() {
	g.keyColumns = make([]Column, 0, len(g.columns))
	g.noneKeyColumns = make([]Column, 0, len(g.columns))

	for _, c := range g.columns {
		if c.ColumnKey == "PRI" || c.ColumnKey == "UNI" {
			g.keyColumns = append(g.keyColumns, c)
		} else {
			g.noneKeyColumns = append(g.noneKeyColumns, c)
		}
	}
}

func (g *daoGenerator) findAutoIncrement() {
	for _, c := range g.columns {
		if strings.Contains(c.Extra, "auto_increment") {
			g.autoIncrement = true
		}
	}
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
