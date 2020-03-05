package sqlmore

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

// DotSQLItem tells the SQL details
type DotSQLItem struct {
	Content string
	Name    string
	Attrs   map[string]string
}

var re = regexp.MustCompile(`\s*(\w+)\s*(:\s*(\S+))?`) // nolint

// ParseDotTag parses the tag like name:value age:34 adult to map
// returns the map and main tag's value
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
	s.current = DotSQLItem{Name: name, Attrs: tag}
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

	current := s.current
	if len(current.Content) > 0 {
		current.Content += "\n"
	}

	current.Content += line

	delimiter := current.Attrs["delimiter"]
	if delimiter == "" {
		delimiter = ";"
	}

	sql := strings.TrimSpace(current.Content)

	for strings.HasPrefix(sql, delimiter) || strings.HasSuffix(sql, delimiter) {
		sql = strings.TrimPrefix(sql, delimiter)
		sql = strings.TrimSuffix(sql, delimiter)
		sql = strings.TrimSpace(sql)
	}

	current.Content = sql

	s.queries[s.current.Name] = current
}

// Run runs the scanner
func (s *DotSQLScanner) Run(io *bufio.Scanner) map[string]DotSQLItem {
	s.queries = make(map[string]DotSQLItem)

	for state := s.initialState; io.Scan(); {
		s.line = io.Text()
		state = state()
	}

	return s.queries
}

// DotSQL is the set of SQL statements
type DotSQL struct {
	Sqls map[string]DotSQLItem
}

// Raw returns the query, everything after the --name tag
func (d DotSQL) Raw(name string) (string, error) { return d.lookupQuery(name) }

func (d DotSQL) lookupQuery(name string) (query string, err error) {
	s, ok := d.Sqls[name]
	if !ok {
		return "", fmt.Errorf("dotsql: '%s' could not be found", name)
	}

	return s.Content, nil
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
