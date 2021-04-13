package sqlx

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/bingoohuang/sqlparser/sqlparser"
	"io"
	"os"
	"reflect"
	"regexp"
	"strings"
	"unicode"

	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	funk "github.com/thoas/go-funk"
)

type SQL struct {
	Query string
	Vars  []interface{}

	CountQuery string
	CuntVars   []interface{}
}

func CreateSQL(baseSQL string, cond interface{}) (*SQL, error) {
	if cond == nil {
		return &SQL{
			Query: baseSQL,
		}, nil
	}

	vc := reflect.ValueOf(cond)
	if vc.Kind() == reflect.Ptr {
		vc = vc.Elem()
	}

	if vc.Kind() != reflect.Struct {
		return nil, errors.New("condition should be struct value or pointer to struct`")
	}

	vt := vc.Type()
	s := &SQL{Query: baseSQL}

	p, err := sqlparser.Parse(baseSQL)
	if err != nil {
		return nil, err
	}

	iw, w := p.(sqlparser.IWhere)
	if w {
		w = iw.GetWhere() != nil
	}

	first := true

	for i := 0; i < vt.NumField(); i++ {
		vtf := vt.Field(i)
		tag := vtf.Tag.Get("sql")
		if tag == "" {
			continue
		}

		fv := vc.Field(i)
		if fv.IsZero() {
			continue
		}

		if vtf.Type.AssignableTo(LimitType) {
			l := fv.Interface().(Limit)
			s.Query += " " + tag
			s.Vars = append(s.Vars, l.Offset, l.Length)
			continue
		}

		if first && !w {
			s.Query += " where " + tag
		} else {
			s.Query += " and " + tag
		}

		first = false

		num := strings.Count(tag, "?")
		if num > 0 {
			fvi := fv.Interface()
			for j := 0; j < num; j++ {
				s.Vars = append(s.Vars, fvi)
			}
		}
	}

	p, err = sqlparser.Parse(s.Query)
	if err != nil {
		return nil, err
	}

	selectQuery, ok := p.(*sqlparser.Select)
	if !ok {
		return s, nil
	}

	selectQuery.SelectExprs = countStarExprs
	selectQuery.OrderBy = nil
	selectQuery.Having = nil
	oldLimit := selectQuery.Limit
	selectQuery.Limit = nil

	limitVarsCount := 0
	if oldLimit != nil {
		limitVarsCount++
		if oldLimit.Offset != nil {
			limitVarsCount++
		}
	}

	s.CountQuery = sqlparser.String(selectQuery)
	s.CuntVars = s.Vars[:len(s.Vars)-limitVarsCount]

	return s, nil
}

// DotSQLItem tells the SQL details.
type DotSQLItem struct {
	Content []string
	Name    string
	Attrs   map[string]string
}

var re = regexp.MustCompile(`\s*(\w+)\s*(:\s*(\S+))?`)

// ParseDotTag parses the tag like name:value age:34 adult to map
// returns the map and main tag's value.
func ParseDotTag(line, prefix, mainTag string) (map[string]string, string) {
	l := strings.TrimSpace(line)
	if !strings.HasPrefix(l, prefix) {
		return nil, ""
	}

	l = strings.TrimSpace(l[2:])
	m := make(map[string]string)

	for _, subs := range re.FindAllStringSubmatch(l, -1) {
		m[subs[1]] = subs[3]
	}

	return m, m[mainTag]
}

// DotSQLScanner scans the SQL statements from .sql files.
type DotSQLScanner struct {
	line    string
	queries map[string]DotSQLItem
	current DotSQLItem
}

func (s *DotSQLScanner) createNewItem(name string, tag map[string]string) {
	s.current = DotSQLItem{Name: name, Attrs: tag, Content: make([]string, 0)}
}

type stateFn func() stateFn

func (s *DotSQLScanner) initialState() stateFn {
	if tag, name := ParseDotTag(s.line, "--", "name"); name != "" {
		s.createNewItem(name, tag)

		return s.queryState
	}

	return s.initialState
}

func (s *DotSQLScanner) queryState() stateFn {
	if tag, name := ParseDotTag(s.line, "--", "name"); name != "" {
		s.createNewItem(name, tag)
	} else {
		s.appendQueryLine()
	}

	return s.queryState
}

func (s *DotSQLScanner) appendQueryLine() {
	line := strings.Trim(s.line, " \t")
	if len(line) == 0 {
		return
	}

	s.current.Content = append(s.current.Content, strings.TrimSpace(line))
	s.queries[s.current.Name] = s.current
}

// Run runs the scanner.
func (s *DotSQLScanner) Run(io *bufio.Scanner) map[string]DotSQLItem {
	s.queries = make(map[string]DotSQLItem)

	for state := s.initialState; io.Scan(); {
		s.line = io.Text()
		state = state()
	}

	return s.queries
}

// DotSQL is the set of SQL statements.
type DotSQL struct {
	Sqls map[string]DotSQLItem
}

// Raw returns the query, everything after the --name tag.
func (d DotSQL) Raw(name string) (SQLPart, error) {
	v, err := d.lookupQuery(name)

	return v, err
}

func (d DotSQL) lookupQuery(name string) (query SQLPart, err error) {
	s, ok := d.Sqls[name]
	if !ok {
		return nil, fmt.Errorf("dotsql: '%s' could not be found", name) // nolint:goerr113
	}

	query, err = s.DynamicSQL()

	return query, err
}

// RawSQL returns the raw SQL.
func (d DotSQLItem) RawSQL() string {
	delimiter := d.Attrs["delimiter"]
	if delimiter == "" {
		delimiter = ";"
	}

	return TrimSQL(strings.Join(d.Content, "\n"), delimiter)
}

// TrimSQL trims the delimiter from the string s.
func TrimSQL(s, delimiter string) string {
	s = strings.TrimSpace(s)

	for strings.HasPrefix(s, delimiter) || strings.HasSuffix(s, delimiter) {
		s = strings.TrimPrefix(s, delimiter)
		s = strings.TrimSuffix(s, delimiter)
		s = strings.TrimSpace(s)
	}

	return s
}

// DynamicSQL returns the dynamic SQL.
func (d DotSQLItem) DynamicSQL() (SQLPart, error) {
	lines := ConvertSQLLines(d.Content)

	_, part, err := ParseDynamicSQL(lines)
	if err != nil {
		return nil, err
	}

	if err := part.Compile(); err != nil {
		return nil, err
	}

	return &PostProcessingSQLPart{
		Part:  part,
		Attrs: d.Attrs,
	}, nil
}

// DotSQLLoad imports sql queries from any io.Reader.
func DotSQLLoad(r io.Reader) (*DotSQL, error) {
	return &DotSQL{(&DotSQLScanner{}).Run(bufio.NewScanner(r))}, nil
}

// DotSQLLoadFile imports SQL queries from the file.
func DotSQLLoadFile(sqlFile string) (*DotSQL, error) {
	f, err := os.Open(sqlFile)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	return DotSQLLoad(f)
}

// DotSQLLoadString imports SQL queries from the string.
func DotSQLLoadString(s string) (*DotSQL, error) { return DotSQLLoad(bytes.NewBufferString(s)) }

// SQLPart defines the dynamic SQL part.
type SQLPart interface {
	// Compile compile the condition int advance.
	Compile() error
	// Eval evaluates the SQL part to a real SQL.
	Eval(m map[string]interface{}) (string, error)
	// Raw returns the raw content.
	Raw() string
}

// PostProcessingSQLPart defines the SQLPart for post-processing like delimiter trimming.
type PostProcessingSQLPart struct {
	Part  SQLPart
	Attrs map[string]string
}

// Compile compile the condition int advance.
func (p *PostProcessingSQLPart) Compile() error {
	return p.Part.Compile()
}

// Eval evaluated the dynamic sql with env.
func (p *PostProcessingSQLPart) Eval(env map[string]interface{}) (string, error) {
	eval, err := p.Part.Eval(env)
	if err != nil {
		return "", err
	}

	delimiter := MapValueOrDefault(p.Attrs, "delimiter", ";")

	return TrimSQL(eval, delimiter), nil
}

// Raw returns the raw content.
func (p *PostProcessingSQLPart) Raw() string {
	raw := p.Part.Raw()

	delimiter := MapValueOrDefault(p.Attrs, "delimiter", ";")

	return TrimSQL(raw, delimiter)
}

// LiteralPart define literal SQL part that no eval required.
type LiteralPart struct {
	Literal string
}

// MakeLiteralPart makes a MakeLiteralPart.
func MakeLiteralPart(s string) SQLPart {
	return &LiteralPart{Literal: s}
}

// Compile compile the condition int advance.
func (p *LiteralPart) Compile() error { return nil }

// Raw returns the raw content.
func (p *LiteralPart) Raw() string { return p.Literal }

// Eval evaluates the SQL part to a real SQL.
func (p *LiteralPart) Eval(map[string]interface{}) (string, error) { return p.Literal, nil }

// IfCondition defines a single condition that makes up a conditions-set for IfPart/SwitchPart.
type IfCondition struct {
	Expr         string
	CompiledExpr *vm.Program
	Part         SQLPart
}

// IfPart is the part that has the format of if ... else if ... else ... end.
type IfPart struct {
	Conditions []IfCondition
	Else       SQLPart
}

// Compile compile the condition int advance.
func (p *IfPart) Compile() (err error) {
	for i, c := range p.Conditions {
		if c.CompiledExpr, err = expr.Compile(c.Expr); err != nil {
			return err
		}

		p.Conditions[i] = c
	}

	return nil
}

// MakeIfPart makes a new IfPart.
func MakeIfPart() *IfPart {
	return &IfPart{Conditions: make([]IfCondition, 0)}
}

// AddElse adds a else part to the IfPart.
func (p *IfPart) AddElse(part SQLPart) {
	p.Else = part
}

// AddCondition adds a condition to the IfPart.
func (p *IfPart) AddCondition(conditionExpr string, part SQLPart) {
	p.Conditions = append(p.Conditions, IfCondition{
		Expr: conditionExpr,
		Part: part,
	})
}

// Eval evaluates the SQL part to a real SQL.
func (p *IfPart) Eval(env map[string]interface{}) (string, error) {
	for _, c := range p.Conditions {
		output, err := expr.Run(c.CompiledExpr, env)
		if err != nil {
			return "", err
		}

		if yes, ok := output.(bool); !ok {
			return "", fmt.Errorf("%s is not a bool expression", c.Expr) // nolint:goerr113
		} else if yes {
			return c.Part.Eval(env)
		}
	}

	if p.Else != nil {
		return p.Else.Eval(env)
	}

	return "", nil
}

// Raw returns the raw content.
func (p *IfPart) Raw() string {
	raw := ""

	for _, c := range p.Conditions {
		raw += c.Expr + "\n" + c.Part.Raw()
	}

	if p.Else != nil {
		raw += "\n" + p.Else.Raw()
	}

	return raw
}

// MultiPart is the multi SQLParts.
type MultiPart struct {
	Parts []SQLPart
}

// MakeMultiPart makes MultiPart.
func MakeMultiPart() *MultiPart {
	return &MultiPart{Parts: make([]SQLPart, 0)}
}

// Eval evaluates the SQL part to a real SQL.
func (p *MultiPart) Eval(env map[string]interface{}) (string, error) {
	value := ""

	for _, p := range p.Parts {
		v, err := p.Eval(env)
		if err != nil {
			return "", err
		}

		if value != "" {
			value += " "
		}

		value += v
	}

	return value, nil
}

// Raw returns the raw content.
func (p *MultiPart) Raw() string {
	raw := ""

	for _, c := range p.Parts {
		if raw != "" {
			raw += "\n"
		}

		raw += c.Raw()
	}

	return raw
}

// AddPart adds a part to the current MultiPart.
func (p *MultiPart) AddPart(part SQLPart) {
	p.Parts = append(p.Parts, part)
}

// Compile compile the condition int advance.
func (p *MultiPart) Compile() error {
	for _, part := range p.Parts {
		if err := part.Compile(); err != nil {
			return err
		}
	}

	return nil
}

var _ SQLPart = (*LiteralPart)(nil)
var _ SQLPart = (*IfPart)(nil)
var _ SQLPart = (*MultiPart)(nil)
var _ SQLPart = (*PostProcessingSQLPart)(nil)

// ParseDynamicSQL parses the dynamic sqls to structured SQLPart.
func ParseDynamicSQL(lines []string, terminators ...string) (int, SQLPart, error) {
	multiPart := MakeMultiPart()

	for i := 0; i < len(lines); i++ {
		l := lines[i]

		if !strings.HasPrefix(l, "--") {
			multiPart.AddPart(MakeLiteralPart(l))
			continue
		}

		commentLine := strings.TrimSpace(l[2:])
		word := firstWord(commentLine, 1)
		parser := CreateParser(word, strings.TrimSpace(commentLine[len(word):]))

		if parser == nil { // no parser found, ignore comment line
			if funk.ContainsString(terminators, word) {
				return i, multiPart, nil
			}

			continue
		}

		partLines, part, err := parser.Parse(lines[i+1:])
		if err != nil {
			return 0, nil, err
		}

		multiPart.AddPart(part)

		i += partLines - 1
	}

	return len(lines), multiPart, nil
}

// ConvertSQLLines converts the inline comments to line comments
// and merge the uncomment lines together.
// nolint:funlen
func ConvertSQLLines(lines []string) []string {
	inlineCommentMode := false
	noneComment := ""
	inlineCommentContent := ""
	converted := make([]string, 0)

	for _, l := range lines {
		if strings.HasPrefix(l, "--") {
			if noneComment != "" {
				converted = append(converted, noneComment)
				noneComment = ""
			}

			converted = append(converted, l)

			continue
		}

	inlineCommentGo:
		l = strings.TrimSpace(l)

		if l == "" {
			continue
		}

		if !inlineCommentMode {
			inlineCommentStart := strings.Index(l, "/*")
			if inlineCommentStart < 0 {
				noneComment = appendNoneComment(noneComment, l)

				continue
			}

			inlineCommentMode = true

			if before := strings.TrimSpace(l[0:inlineCommentStart]); before != "" {
				noneComment = appendNoneComment(noneComment, before)
			}

			l = l[inlineCommentStart+2:]
		}

		inlineCommentStop := strings.Index(l, "*/")
		if inlineCommentStop >= 0 {
			inlineCommentMode = false
			inlineCommentContent += l[:inlineCommentStop]

			if inlineComment := strings.TrimSpace(inlineCommentContent); inlineComment != "" {
				if noneComment != "" {
					converted = append(converted, noneComment)
					noneComment = ""
				}

				converted = append(converted, "-- "+inlineComment)
			}

			l = l[inlineCommentStop+2:]
			inlineCommentContent = ""

			goto inlineCommentGo
		}

		inlineCommentContent += l
	}

	if noneComment != "" {
		converted = append(converted, noneComment)
	}

	return converted
}

func appendNoneComment(noneComment string, l string) string {
	if noneComment != "" {
		noneComment += "\n"
	}

	return noneComment + l
}

// SQLPartParser defines the parser of SQLPart.
type SQLPartParser interface {
	// Parse parses the lines to SQLPart.
	Parse(lines []string) (partLines int, part SQLPart, err error)
}

// IfSQLPartParser defines the Parser of IfPart.
type IfSQLPartParser struct {
	Condition string
	Else      string
}

// MakeIfSQLPartParser makes a IfSQLPartParser.
func MakeIfSQLPartParser(condition string) *IfSQLPartParser {
	return &IfSQLPartParser{
		Condition: condition,
	}
}

// Parse parses the lines to SQLPart.
func (p *IfSQLPartParser) Parse(lines []string) (partLines int, part SQLPart, err error) {
	ifPart := MakeIfPart()
	condition := p.Condition

	for i := 0; i < len(lines); i++ {
		l := lines[i]

		if !strings.HasPrefix(l, "--") {
			ifPart.AddCondition(condition, MakeLiteralMultiPart(l))
			continue
		}

		commentLine := strings.TrimSpace(l[2:])
		word := firstWord(commentLine, 1)

		if word == "end" {
			return i + 2 /*包括if 行*/, ifPart, nil
		}

		if word == "elseif" {
			condition = strings.TrimSpace(commentLine[len(word):])

			processLines, sqlPart, err := ParseDynamicSQL(lines[i+1:], "end", "elseif", "else")
			if err != nil {
				return 0, nil, err
			}

			ifPart.AddCondition(condition, sqlPart)

			i += processLines

			continue
		}

		if word == "else" {
			processLines, sqlPart, err := ParseDynamicSQL(lines[i+1:], "end")
			if err != nil {
				return 0, nil, err
			}

			ifPart.AddElse(sqlPart)

			return i + 2 + processLines, ifPart, nil
		}

		processLines, sqlPart, err := ParseDynamicSQL(lines[i:], "end", "elseif", "else")
		if err != nil {
			return 0, nil, err
		}

		ifPart.AddCondition(condition, sqlPart)

		i += processLines - 1
	}

	return 0, nil, fmt.Errorf("no end found for if expr") // nolint:goerr113
}

// MakeLiteralMultiPart makes a MultiPart.
func MakeLiteralMultiPart(l string) *MultiPart {
	return &MultiPart{Parts: []SQLPart{&LiteralPart{l}}}
}

var _ SQLPartParser = (*IfSQLPartParser)(nil)

// CreateParser creates a SQLPartParser.
// If no parser found, nil returned.
func CreateParser(word string, l string) SQLPartParser {
	if word == "if" {
		return MakeIfSQLPartParser(l)
	}

	return nil
}

func firstWord(value string, count int) string {
	// Loop over all indexes in the string.
	for i := range value {
		// If we encounter a space, reduce the count.
		if unicode.IsSpace(rune(value[i])) {
			count--
			// When no more words required, return a substring.
			if count == 0 {
				return value[0:i]
			}
		}
	}

	// Return the entire string.
	return value
}
