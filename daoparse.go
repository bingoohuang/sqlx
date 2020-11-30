package sqlx

import (
	"fmt"
	"regexp"
	"strconv"
)

// nolint:goerr113
func (p *SQLParsed) checkFuncInOut(numIn int, f StructField) error {
	if numIn == 0 && !p.isBindBy(ByNone) {
		return fmt.Errorf("sql %s required bind varialbes, but the func %v has none", p.Stmt, f.Type)
	}

	if numIn != 1 && p.isBindBy(ByName) {
		return fmt.Errorf("sql %s required named varialbes, but the func %v has non-one arguments",
			p.Stmt, f.Type)
	}

	if p.isBindBy(BySeq, ByAuto) {
		if numIn < p.MaxSeq {
			// nolint:goerr113
			return fmt.Errorf("sql %s required max %d vars, but the func %v has only %d arguments",
				p.Stmt, p.MaxSeq, f.Type, numIn)
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

	opt  *CreateDaoOpt
	Stmt string
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

// ParseSQL parses the sql.
func ParseSQL(sqlName, stmt string) (*SQLParsed, error) {
	p := &SQLParsed{}

	if err := p.parseSQL(sqlName, stmt); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *SQLParsed) parseSQL(sqlName, stmt string) error {
	p.Vars = make([]string, 0)
	p.Stmt = sqlre.ReplaceAllStringFunc(stmt, func(v string) string {
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

	if p.opt != nil && p.opt.DBGetter != nil {
		p.Stmt = convertSQLBindMarks(p.opt.DBGetter.GetDB(), p.Stmt)
	}

	var err error

	p.BindBy, p.MaxSeq, err = parseBindBy(sqlName, p.Vars)
	if err != nil {
		return err
	}

	_, p.IsQuery = IsQuerySQL(p.Stmt)

	return nil
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
