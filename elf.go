package sqlx

import (
	"github.com/bingoohuang/sqlparser/sqlparser"
	"github.com/bingoohuang/strcase"
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

// FixPkgName fixes the package name to all lower case with letters and digits kept.
func FixPkgName(pkgName string) string {
	name := ""
	started := false

	for _, c := range pkgName {
		if !started {
			started = unicode.IsLetter(c)
		}

		if started {
			if unicode.IsLetter(c) || unicode.IsDigit(c) {
				name += string(unicode.ToLower(c))
			}
		}
	}

	return name
}

func remove(slice []reflect.Type, s int) []reflect.Type {
	if s < 0 {
		return slice
	}

	return append(slice[:s], slice[s+1:]...)
}

func insert(a []reflect.Value, index int, value reflect.Value) []reflect.Value {
	if len(a) == index { // nil or empty slice or after last element
		return append(a, value)
	}

	a = append(a[:index+1], a[index:]...) // index < len(a)
	a[index] = value
	return a
}

var countStarExprs = func() sqlparser.SelectExprs {
	p, _ := sqlparser.Parse(`select count(*)`)
	return p.(*sqlparser.Select).SelectExprs
}()

func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}

func convertSQLBindMarks(db SqlDB, s string) string {
	driverName := LookupDriverName(db.Driver())
	switch driverName {
	case "postgres":
		return replaceQuestionMark4Postgres(s)
	default:
		return s
	}
}

func replaceQuestionMark4Postgres(s string) string {
	r := ""

	for seq := 1; ; seq++ {
		pos := strings.Index(s, "?")
		if pos < 0 {
			r += s
			break
		}

		r += s[0:pos] + "$" + strconv.Itoa(seq)
		s = s[pos+1:]
	}

	return r
}

func matchesField2Col(structType reflect.Type, field, col string) bool {
	f, _ := structType.FieldByName(field)
	if tagName := f.Tag.Get("name"); tagName != "" {
		return tagName == col
	}

	return strings.EqualFold(field, col) || strings.EqualFold(field, strcase.ToCamel(col))
}
