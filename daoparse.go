package sqlx

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/bingoohuang/sqlparser/sqlparser"
)

// nolint:goerr113
func (p *SQLParsed) checkFuncInOut(numIn int, f StructField) error {
	if numIn == 0 && !p.isBindBy(ByNone) {
		return fmt.Errorf("sql %s required bind varialbes, but the func %v has none", p.RawStmt, f.Type)
	}

	if numIn != 1 && p.isBindBy(ByName) {
		return fmt.Errorf("sql %s required named varialbes, but the func %v has non-one arguments",
			p.RawStmt, f.Type)
	}

	if p.isBindBy(BySeq, ByAuto) {
		if numIn < p.MaxSeq {
			// nolint:goerr113
			return fmt.Errorf("sql %s required max %d vars, but the func %v has only %d arguments",
				p.RawStmt, p.MaxSeq, f.Type, numIn)
		}
	}

	return nil
}

type bindBy int

const (
	// ByNone means no bind params.
	ByNone bindBy = iota
	// ByAuto means auto seq for bind params.
	ByAuto
	// BySeq means specific seq for bind params.
	BySeq
	// ByName means named bind params.
	ByName
)

func (b bindBy) String() string {
	switch b {
	case ByNone:
		return "byNone"
	case ByAuto:
		return "byAuto"
	case BySeq:
		return "bySeq"
	case ByName:
		return "byName"
	default:
		return "Unknown"
	}
}

// SQLParsed is the structure of the parsed SQL.
type SQLParsed struct {
	ID      string
	SQL     SQLPart
	BindBy  bindBy
	Vars    []string
	MaxSeq  int
	IsQuery bool

	opt     *CreateDaoOpt
	RawStmt string

	fp     FieldParts
	runSQL string
}

func (p SQLParsed) isBindBy(by ...bindBy) bool {
	for _, b := range by {
		if p.BindBy == b {
			return true
		}
	}

	return false
}

var sqlre = regexp.MustCompile(`'?:\w*'?`)

type FieldParts struct {
	fieldParts []FieldPart
	fieldVars  []interface{}
}

func (p *FieldParts) AddFieldSqlPart(part string, varVal []interface{}, joinedSep bool) {
	p.fieldParts = append(p.fieldParts, FieldPart{
		PartSQL:        part,
		BindVal:        varVal,
		PartSQLPlTimes: strings.Count(part, "?"),
		JoinedSep:      joinedSep,
	})
}

// ParseSQL parses the sql.
func ParseSQL(name, stmt string) (*SQLParsed, error) {
	p := &SQLParsed{ID: name}

	if err := p.fastParseSQL(stmt); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *SQLParsed) fastParseSQL(stmt string) error {
	p.Vars = make([]string, 0)
	p.RawStmt = sqlre.ReplaceAllStringFunc(stmt, func(v string) string {
		if v[0:1] == "'" {
			v = v[2:]
		} else {
			v = v[1:]
		}

		if v != "" && v[len(v)-1:] == "'" {
			v = v[:len(v)-1]
		}

		p.Vars = append(p.Vars, v)
		return "?"
	})

	var err error

	p.BindBy, p.MaxSeq, err = parseBindBy(p.ID, p.Vars)
	if err != nil {
		return err
	}

	_, p.IsQuery = IsQuerySQL(p.RawStmt)
	return nil
}

func (p *SQLParsed) parseSQL(runSQl string) error {
	p.Vars = make([]string, 0)
	p.runSQL = sqlre.ReplaceAllStringFunc(runSQl, func(v string) string {
		if v[0:1] == "'" {
			v = v[2:]
		} else {
			v = v[1:]
		}

		if v != "" && v[len(v)-1:] == "'" {
			v = v[:len(v)-1]
		}

		p.Vars = append(p.Vars, v)
		return "?"
	})

	if len(p.fp.fieldParts) > 0 {
		parsed, err := sqlparser.Parse(p.runSQL)
		if err != nil {
			panic(err)
		}

		w, hasWhere := parsed.(sqlparser.IWhere)
		if hasWhere {
			hasWhere = w.GetWhere() != nil
		}

		for i, f := range p.fp.fieldParts {
			if f.JoinedSep {
				if i == 0 && !hasWhere {
					p.runSQL += " where " + f.PartSQL
				} else {
					p.runSQL += " and " + f.PartSQL
				}
			} else {
				p.runSQL += " " + f.PartSQL
			}

			p.Vars = append(p.Vars, f.VarMarks()...)
			p.fp.fieldVars = append(p.fp.fieldVars, f.Vars()...)
		}
	}

	if p.opt != nil && p.opt.DBGetter != nil {
		p.runSQL = convertSQLBindMarks(p.opt.DBGetter.GetDB(), p.runSQL)
	}

	return nil
}

type FieldPart struct {
	PartSQL        string
	BindVal        []interface{}
	PartSQLPlTimes int
	JoinedSep      bool
}

func (p FieldPart) VarMarks() []string {
	vars := make([]string, p.PartSQLPlTimes)

	for i := 0; i < p.PartSQLPlTimes; i++ {
		vars[i] = "?"
	}

	return vars
}

func (p FieldPart) Vars() []interface{} {
	vars := make([]interface{}, p.PartSQLPlTimes)

	for i := 0; i < p.PartSQLPlTimes; i++ {
		vars[i] = p.BindVal[i]
	}

	return vars
}

func parseBindBy(sqlName string, vars []string) (bindBy bindBy, maxSeq int, err error) {
	bindBy = ByNone

	for _, v := range vars {
		if v == "" {
			if bindBy == ByAuto {
				maxSeq++
				continue
			}

			if bindBy != ByNone {
				// nolint:goerr113
				return 0, 0, fmt.Errorf("[%s] illegal mixed bind mod (%v-%v)", sqlName, bindBy, ByAuto)
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
				// nolint:goerr113
				return 0, 0, fmt.Errorf("[%s] illegal mixed bind mod (%v-%v)", sqlName, bindBy, BySeq)
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
			// nolint:goerr113
			return 0, 0, fmt.Errorf("[%s] illegal mixed bind mod (%v-%v)", sqlName, bindBy, ByName)
		}

		bindBy = ByName
		maxSeq++
	}

	return bindBy, maxSeq, nil
}
