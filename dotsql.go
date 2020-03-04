package sqlmore

import (
	"bufio"
	"regexp"
	"strings"
)

// DotSQL tells the SQL details
type DotSQL struct {
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
	queries map[string]DotSQL
	current DotSQL
}

type stateFn func() stateFn

func (s *DotSQLScanner) initialState() stateFn {
	if tag, name := ParseDotTag(s.line, "--", "name"); name != "" {
		s.current = DotSQL{Name: name, Attrs: tag}

		return s.queryState
	}

	return s.initialState
}

func (s *DotSQLScanner) queryState() stateFn {
	if tag, name := ParseDotTag(s.line, "--", "name"); name != "" {
		s.current = DotSQL{Name: name, Attrs: tag}
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

	current := s.queries[s.current.Name]
	if len(current.Content) > 0 {
		current.Content += "\n"
	}

	current.Content += line
	s.queries[s.current.Name] = current
}

// Run runs the scanner
func (s *DotSQLScanner) Run(io *bufio.Scanner) map[string]DotSQL {
	s.queries = make(map[string]DotSQL)

	for state := s.initialState; io.Scan(); {
		s.line = io.Text()
		state = state()
	}

	return s.queries
}
