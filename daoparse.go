package sqlx

import (
	"fmt"
	"regexp"
	"strconv"
)

func (p *sqlParsed) checkFuncInOut(numIn int, f StructField) error {
	if numIn == 0 && !p.isBindBy(byNone) {
		return fmt.Errorf("sql %s required bind varialbes, but the func %v has none", p.Stmt, f.Type)
	}

	if numIn != 1 && p.isBindBy(byName) {
		return fmt.Errorf("sql %s required named varialbes, but the func %v has non-one arguments",
			p.Stmt, f.Type)
	}

	if p.isBindBy(bySeq, byAuto) {
		if numIn < p.MaxSeq {
			return fmt.Errorf("sql %s required max %d vars, but the func %v has only %d arguments",
				p.Stmt, p.MaxSeq, f.Type, numIn)
		}
	}

	return nil
}

type bindBy int

const (
	byNone bindBy = iota
	byAuto
	bySeq
	byName
)

func (b bindBy) String() string {
	switch b {
	case byNone:
		return "byNone"
	case byAuto:
		return "byAuto"
	case bySeq:
		return "bySeq"
	case byName:
		return "byName"
	default:
		return "Unknown"
	}
}

type sqlParsed struct {
	ID      string
	SQL     SQLPart
	BindBy  bindBy
	Vars    []string
	MaxSeq  int
	IsQuery bool

	opt       *CreateDaoOpt
	sqlFilter func(s string) string
	Stmt      string
}

func (p sqlParsed) isBindBy(by ...bindBy) bool {
	for _, b := range by {
		if p.BindBy == b {
			return true
		}
	}

	return false
}

var sqlre = regexp.MustCompile(`'?:\w*'?`) // nolint gochecknoglobals

func parseSQL(sqlName, stmt string) (*sqlParsed, error) {
	p := &sqlParsed{}

	if err := p.parseSQL(sqlName, stmt); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *sqlParsed) parseSQL(sqlName, stmt string) error {
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

	var err error

	p.BindBy, p.MaxSeq, err = parseBindBy(sqlName, p.Vars)
	if err != nil {
		return err
	}

	_, p.IsQuery = IsQuerySQL(p.Stmt)

	return nil
}

func parseBindBy(sqlName string, vars []string) (bindBy bindBy, maxSeq int, err error) {
	bindBy = byNone

	for _, v := range vars {
		if v == "" {
			if bindBy == byAuto {
				maxSeq++
				continue
			}

			if bindBy != byNone {
				return 0, 0, fmt.Errorf("[%s] illegal mixed bind mod (%v-%v)", sqlName, bindBy, byAuto)
			}

			bindBy = byAuto
			maxSeq++

			continue
		}

		n, err := strconv.Atoi(v)
		if err == nil {
			if bindBy == bySeq {
				if maxSeq < n {
					maxSeq = n
				}

				continue
			}

			if bindBy != byNone {
				return 0, 0, fmt.Errorf("[%s] illegal mixed bind mod (%v-%v)", sqlName, bindBy, bySeq)
			}

			bindBy = bySeq
			maxSeq = n

			continue
		}

		if bindBy == byName {
			maxSeq++
			continue
		}

		if bindBy != byNone {
			return 0, 0, fmt.Errorf("[%s] illegal mixed bind mod (%v-%v)", sqlName, bindBy, byName)
		}

		bindBy = byName
		maxSeq++
	}

	return bindBy, maxSeq, nil
}
