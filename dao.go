package sqlmore

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/bingoohuang/strcase"
)

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

// CreateDao fulfils the dao (should be pointer)
func CreateDao(driverName string, db *sql.DB, dao interface{}) error {
	sqlFilter := func(s string) string {
		if driverName == "postgres" {
			return replaceQuestionMark4Postgres(s)
		}

		return s
	}

	v := reflect.ValueOf(dao)
	v = reflect.Indirect(v)

	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)

		if f.PkgPath != "" /* not exportable?*/ || f.Type.Kind() != reflect.Func {
			continue
		}

		sqlStmt := f.Tag.Get("sql")
		p, err := parseSQL(f.Name, sqlStmt)
		p.SQL = sqlFilter(p.SQL)

		if err != nil {
			return fmt.Errorf("failed to parse sql %v error %v", sqlStmt, err)
		}

		numIn := f.Type.NumIn()
		numOut := f.Type.NumOut()
		_, isQuerySQL := IsQuerySQL(sqlStmt)

		if err := p.checkFuncInOut(numIn, sqlStmt, f); err != nil {
			return err
		}

		p.createFn(f, numIn, numOut, db, isQuerySQL, v.Field(i))
	}

	return nil
}

func (p *sqlParsed) createFn(f reflect.StructField, numIn, numOut int, db *sql.DB, isQuerySQL bool, v reflect.Value) {
	var fn func([]reflect.Value) ([]reflect.Value, error)

	switch {
	case numIn == 0 && numOut == 0:
		fn = func([]reflect.Value) ([]reflect.Value, error) { return p.exec(db) }
	case numIn == 1 && p.isBindBy(byName) && numOut == 0:
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return p.execByNamedArg0Ret1(db, args[0]) }
	case isQuerySQL && p.isBindBy(bySeq, byAuto, byNone) && numOut == 1:
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return p.queryBySeqRet1(db, f.Type.Out(0), args) }
	case !isQuerySQL && p.isBindBy(bySeq, byAuto) && numOut == 1:
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return p.execBySeqRet1(db, f.Type.Out(0), args) }
	}

	if fn != nil {
		v.Set(reflect.MakeFunc(f.Type, func(args []reflect.Value) (results []reflect.Value) {
			values, err := fn(args)
			if err != nil {
				fmt.Fprintf(os.Stderr, "execute %s error %v\n", p.SQL, err)
			}

			return values
		}))
	}
}

func (p *sqlParsed) checkFuncInOut(numIn int, sqlStmt string, f reflect.StructField) error {
	if numIn == 0 && !p.isBindBy(byNone) {
		return fmt.Errorf("sql %s required bind varialbes, but the func %v has none", sqlStmt, f.Type)
	}

	if numIn != 1 && p.isBindBy(byName) {
		return fmt.Errorf("sql %s required named varialbes, but the func %v has non-one arguments",
			sqlStmt, f.Type)
	}

	if p.isBindBy(bySeq, byAuto) {
		if numIn < p.MaxSeq {
			return fmt.Errorf("sql %s required max %d vars, but the func %v has only %d arguments",
				sqlStmt, p.MaxSeq, f.Type, numIn)
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
	}

	return "Unknown"
}

type sqlParsed struct {
	ID     string
	SQL    string
	BindBy bindBy
	Vars   []string
	MaxSeq int
}

func (p sqlParsed) isBindBy(by ...bindBy) bool {
	for _, b := range by {
		if p.BindBy == b {
			return true
		}
	}

	return false
}

var sqlre = regexp.MustCompile(`:\w*`) // nolint gochecknoglobals

func parseSQL(sqlID, stmt string) (*sqlParsed, error) {
	vars := make([]string, 0)
	parsed := sqlre.ReplaceAllStringFunc(stmt, func(bindVar string) string {
		vars = append(vars, bindVar[1:])
		return "?"
	})

	bindBy, maxSeq, err := parseBindBy(vars)
	if err != nil {
		return nil, err
	}

	return &sqlParsed{
		ID:     sqlID,
		SQL:    parsed,
		BindBy: bindBy,
		Vars:   vars,
		MaxSeq: maxSeq,
	}, nil
}

func parseBindBy(vars []string) (bindBy bindBy, maxSeq int, err error) {
	bindBy = byNone

	for _, v := range vars {
		if v == "" {
			if bindBy == byAuto {
				maxSeq++
				continue
			}

			if bindBy != byNone {
				return 0, 0, fmt.Errorf("illegal mixed bind mod (%v-%v)", bindBy, byAuto)
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
				return 0, 0, fmt.Errorf("illegal mixed bind mod (%v-%v)", bindBy, bySeq)
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
			return 0, 0, fmt.Errorf("illegal mixed bind mod (%v-%v)", bindBy, byName)
		}

		bindBy = byName
		maxSeq++
	}

	return bindBy, maxSeq, nil
}

func (p *sqlParsed) exec(db *sql.DB) ([]reflect.Value, error) {
	p.logPrepare("(none)")
	_, err := db.Exec(p.SQL)

	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", p.SQL, err)
	}

	return []reflect.Value{}, nil
}

func matchesField2Col(structType reflect.Type, field, col string) bool {
	f, _ := structType.FieldByName(field)
	if tagName := f.Tag.Get("name"); tagName != "" {
		return tagName == col
	}

	return strings.EqualFold(field, col) || strings.EqualFold(field, strcase.ToCamel(col))
}

func (p *sqlParsed) execByNamedArg0Ret1(db *sql.DB, bean reflect.Value) ([]reflect.Value, error) {
	beanType := bean.Type()
	isBeanSlice := beanType.Kind() == reflect.Slice
	item0 := bean
	itemSize := 1

	if isBeanSlice {
		if bean.IsNil() || bean.Len() == 0 {
			return []reflect.Value{}, nil
		}

		beanType = beanType.Elem()
		item0 = bean.Index(0)
		itemSize = bean.Len()
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx %w", err)
	}

	pr, err := tx.Prepare(p.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare sql %s error %w", p.SQL, err)
	}

	vars := p.createVars(itemSize, item0, bean, beanType)

	if isBeanSlice {
		p.logPrepare(vars)
	} else {
		p.logPrepare(vars[0])
	}

	for ii := 0; ii < itemSize; ii++ {
		if _, err := pr.Exec(vars[ii]...); err != nil {
			return nil, fmt.Errorf("failed to execute %s error %w", p.SQL, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commiterror %w", err)
	}

	return []reflect.Value{}, nil
}

func (p *sqlParsed) createVars(beanSize int, item, bean reflect.Value, itemType reflect.Type) [][]interface{} {
	vars := make([][]interface{}, beanSize)

	for ii := 0; ii < beanSize; ii++ {
		vars[ii] = make([]interface{}, len(p.Vars))

		if ii > 0 {
			item = bean.Index(ii)
		}

		for i, name := range p.Vars {
			name := name
			fv := item.FieldByNameFunc(func(f string) bool { return matchesField2Col(itemType, f, name) })
			vars[ii][i] = fv.Interface()
		}
	}

	return vars
}

func (p *sqlParsed) logPrepare(vars interface{}) {
	fmt.Printf("start to exec %s: %s with args %v\n", p.ID, p.SQL, vars)
}

func (p *sqlParsed) execBySeqRet1(db *sql.DB, outType reflect.Type, args []reflect.Value) ([]reflect.Value, error) {
	vars := p.makeVars(args)
	p.logPrepare(vars)

	result, err := db.Exec(p.SQL, vars...)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", p.SQL, err)
	}

	affected, err := convertRowsAffected(result, p.SQL, outType)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", p.SQL, err)
	}

	return []reflect.Value{affected}, nil
}

func (p *sqlParsed) queryBySeqRet1(db *sql.DB, outType reflect.Type, args []reflect.Value) ([]reflect.Value, error) {
	vars := p.makeVars(args)
	isOutSlice := outType.Kind() == reflect.Slice
	outSlice := reflect.Value{}

	if isOutSlice {
		outSlice = reflect.MakeSlice(outType, 0, 0)
		outType = outType.Elem()
	}

	p.logPrepare(vars)

	rows, err := db.Query(p.SQL, vars...)
	if err != nil || rows.Err() != nil {
		if err == nil {
			err = rows.Err()
		}

		return nil, fmt.Errorf("execute %s error %w", p.SQL, err)
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns %s error %w", p.SQL, err)
	}

	mapFields := p.createMapFields(columns, outType)

	for ri := 0; rows.Next(); ri++ {
		pointers, out := resetDests(outType, mapFields)
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan rows %s error %w", p.SQL, err)
		}

		for i, field := range mapFields {
			if field != nil {
				f := out.FieldByName(field.Name)
				f.Set(reflect.Indirect(reflect.ValueOf(pointers[i])))
			}
		}

		if !isOutSlice {
			return []reflect.Value{out}, nil
		}

		outSlice = reflect.Append(outSlice, out)
	}

	if isOutSlice {
		return []reflect.Value{outSlice}, nil
	}

	return []reflect.Value{reflect.Indirect(reflect.New(outType))}, nil
}

func resetDests(outType reflect.Type, mapFields []*reflect.StructField) ([]interface{}, reflect.Value) {
	pointers := make([]interface{}, len(mapFields))
	out := reflect.Indirect(reflect.New(outType))

	for i, fv := range mapFields {
		if fv != nil {
			pointers[i] = reflect.New(fv.Type).Interface()
		} else {
			pointers[i] = &sql.NullString{}
		}
	}

	return pointers, out
}

func (p *sqlParsed) createMapFields(columns []string, outType reflect.Type) []*reflect.StructField {
	mapFields := make([]*reflect.StructField, len(columns))

	for i, col := range columns {
		col := col
		fv, ok := outType.FieldByNameFunc(func(field string) bool { return matchesField2Col(outType, field, col) })

		if ok {
			mapFields[i] = &fv
		}
	}

	return mapFields
}

func (p *sqlParsed) makeVars(args []reflect.Value) []interface{} {
	vars := make([]interface{}, 0, len(p.Vars))

	for i, name := range p.Vars {
		if p.BindBy == byAuto {
			vars = append(vars, args[i].Interface())
		} else {
			seq, _ := strconv.Atoi(name)
			vars = append(vars, args[seq-1].Interface())
		}
	}

	return vars
}

func convertRowsAffected(result sql.Result, stmt string, outType reflect.Type) (reflect.Value, error) {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return reflect.Value{}, fmt.Errorf("rowsAffected %s error %w", stmt, err)
	}

	rowsAffectedV := reflect.ValueOf(rowsAffected)
	if rowsAffectedV.Type().ConvertibleTo(outType) {
		return rowsAffectedV.Convert(outType), nil
	}

	return reflect.Value{}, fmt.Errorf("unable to convert %v to type %v", rowsAffected, outType)
}
