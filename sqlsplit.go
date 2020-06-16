package sqlx

import (
	"strings"
	"unicode/utf8"
)

// SplitSqls splits sqls by separate.
func SplitSqls(sqls string, separate rune) []string {
	subs := make([]string, 0)

	inQuoted := false
	pos := 0
	l := len(sqls)

	var runeValue rune
	for i, w := 0, 0; i < l; i += w {
		runeValue, w = utf8.DecodeRuneInString(sqls[i:])

		var nextRuneValue rune

		nextWidth := 0

		if i+w < l {
			nextRuneValue, nextWidth = utf8.DecodeRuneInString(sqls[i+w:])
		}

		jumpNext := false

		switch {
		case runeValue == '\\':
			jumpNext = true
		case runeValue == '\'':
			if inQuoted && nextWidth > 0 && nextRuneValue == '\'' {
				jumpNext = true // jump escape for literal apostrophe, or single quote
			} else {
				inQuoted = !inQuoted
			}
		case !inQuoted && runeValue == separate:
			subs = tryAddSQL(subs, sqls[pos:i])
			pos = i + w
		}

		if jumpNext {
			i += w + nextWidth
		}
	}

	if pos < l {
		subs = tryAddSQL(subs, sqls[pos:])
	}

	return subs
}

func tryAddSQL(sqls []string, sql string) []string {
	s := strings.TrimSpace(sql)
	if s != "" {
		return append(sqls, s)
	}

	return sqls
}
