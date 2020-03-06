package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/bingoohuang/goreflect/defaults"

	"github.com/bingoohuang/strcase"
)

// CreateDao fulfils the dao (should be pointer)
func CreateDao(driverName string, db *sql.DB, dao interface{}, createDaoOpts ...CreateDaoOpter) error {
	option, err := applyCreateDaoOption(createDaoOpts)
	if err != nil {
		return err
	}

	sqlFilter := func(s string) string {
		switch driverName {
		case "postgres":
			return replaceQuestionMark4Postgres(s)
		default:
			return s
		}
	}

	v := reflect.Indirect(reflect.ValueOf(dao))
	errSetter := createErrorSetter(v, option)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		f := v.Type().Field(i)

		if f.PkgPath != "" /* not exportable */ || f.Type.Kind() != reflect.Func {
			continue
		}

		sqlStmt := f.Tag.Get("sql")
		if sqlStmt == "" && option.DotSQL != nil {
			sqlStmt, _ = option.DotSQL.Raw(f.Name)
		}

		if sqlStmt == "" {
			return fmt.Errorf("failed to find sql with name %s", f.Name)
		}

		p, err := parseSQL(f.Name, sqlStmt)
		if err != nil {
			return fmt.Errorf("failed to parse sql %v error %w", sqlStmt, err)
		}

		p.opt = option
		p.SQL = sqlFilter(p.SQL)
		numIn := f.Type.NumIn()

		if err := p.checkFuncInOut(numIn, sqlStmt, f); err != nil {
			return err
		}

		if err := p.createFn(f, db, field, errSetter); err != nil {
			return err
		}
	}

	return nil
}

func applyCreateDaoOption(createDaoOpts []CreateDaoOpter) (*CreateDaoOpt, error) {
	opt := &CreateDaoOpt{}
	if err := defaults.Set(opt); err != nil {
		return nil, fmt.Errorf("failed to set defaults for CreateDaoOpt error %w", err)
	}

	for _, v := range createDaoOpts {
		v.ApplyCreateOpt(opt)
	}

	if opt.Ctx == nil {
		opt.Ctx = context.Background()
	}

	return opt, nil
}

func createErrorSetter(v reflect.Value, option *CreateDaoOpt) func(error) {
	for i := 0; i < v.NumField(); i++ {
		fv := v.Field(i)
		f := v.Type().Field(i)

		if f.PkgPath == "" /* exportable */ && IsError(f.Type) {
			return func(err error) {
				if option.Error != nil {
					*option.Error = err
				}

				if fv.IsNil() && err == nil {
					return
				}

				if err == nil {
					fv.Set(reflect.Zero(f.Type))
				} else {
					fv.Set(reflect.ValueOf(err))
				}
			}
		}
	}

	return func(err error) {
		if option.Error != nil {
			*option.Error = err
		}
	}
}

func (p *sqlParsed) createFn(f reflect.StructField, db *sql.DB, v reflect.Value, errSetter errorSetter) error {
	numIn := f.Type.NumIn()
	numOut := f.Type.NumOut()

	lastOutError := numOut > 0 && IsError(f.Type.Out(numOut-1)) // nolint gomnd
	if lastOutError {
		numOut--
	}

	var fn func([]reflect.Value) ([]reflect.Value, error)

	switch {
	case numIn == 0 && numOut == 0:
		fn = func([]reflect.Value) ([]reflect.Value, error) { return p.exec(db) }
	case numIn == 1 && p.isBindBy(byName) && numOut == 0:
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return p.execByNamedArg1Ret0(db, args[0]) }
	case p.isBindBy(bySeq, byAuto) && numOut == 0:
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return p.execBySeqArgsRet0(db, args) }
	case p.IsQuery && p.isBindBy(bySeq, byAuto, byNone) && numOut == 1:
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return p.queryBySeqRet1(db, f.Type.Out(0), args) }
	case !p.IsQuery && p.isBindBy(bySeq, byAuto) && numOut == 1:
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return p.execBySeqRet1(db, f.Type.Out(0), args) }
	}

	if fn == nil {
		err := fmt.Errorf("unsupportd func %v", f.Type)
		p.logError(err)

		return err
	}

	v.Set(reflect.MakeFunc(f.Type, func(args []reflect.Value) []reflect.Value {
		errSetter(nil)
		values, err := fn(args)
		if err != nil {
			errSetter(err)
			p.logError(err)

			values = make([]reflect.Value, numOut, numOut+1) // nolint gomnd
			for i := 0; i < numOut; i++ {
				values[i] = reflect.Zero(f.Type.Out(i))
			}
		}

		if lastOutError {
			values = append(values, reflect.ValueOf(err))
		}

		return values
	}))

	return nil
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
	default:
		return "Unknown"
	}
}

type sqlParsed struct {
	ID      string
	SQL     string
	BindBy  bindBy
	Vars    []string
	MaxSeq  int
	IsQuery bool

	opt *CreateDaoOpt
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

func parseSQL(sqlID, stmt string) (*sqlParsed, error) {
	vars := make([]string, 0)
	parsed := sqlre.ReplaceAllStringFunc(stmt, func(v string) string {
		if v[0:1] == "'" {
			v = v[2:]
		} else {
			v = v[1:]
		}

		if v != "" && v[len(v)-1:] == "'" {
			v = v[:len(v)-1]
		}

		vars = append(vars, v)
		return "?"
	})

	bindBy, maxSeq, err := parseBindBy(vars)
	if err != nil {
		return nil, err
	}

	_, isQuery := IsQuerySQL(parsed)

	return &sqlParsed{
		ID:      sqlID,
		SQL:     parsed,
		BindBy:  bindBy,
		Vars:    vars,
		MaxSeq:  maxSeq,
		IsQuery: isQuery,
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
	_, err := db.ExecContext(p.opt.Ctx, p.SQL)

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

func (p *sqlParsed) execBySeqArgsRet0(db *sql.DB, args []reflect.Value) ([]reflect.Value, error) {
	vars := p.makeVars(args)
	p.logPrepare(vars)

	_, err := db.ExecContext(p.opt.Ctx, p.SQL, vars...)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", p.SQL, err)
	}

	return []reflect.Value{}, nil
}

func (p *sqlParsed) execByNamedArg1Ret0(db *sql.DB, bean reflect.Value) ([]reflect.Value, error) {
	item0 := bean
	itemSize := 1
	isBeanSlice := bean.Type().Kind() == reflect.Slice

	if isBeanSlice {
		if bean.IsNil() || bean.Len() == 0 {
			return []reflect.Value{}, nil
		}

		item0 = bean.Index(0)
		itemSize = bean.Len()
	}

	tx, err := db.BeginTx(p.opt.Ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx %w", err)
	}

	pr, err := tx.PrepareContext(p.opt.Ctx, p.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare sql %s error %w", p.SQL, err)
	}

	vars := p.createNamedVars(itemSize, item0, bean)

	if isBeanSlice {
		p.logPrepare(vars)
	} else {
		p.logPrepare(vars[0])
	}

	for ii := 0; ii < itemSize; ii++ {
		if _, err := pr.ExecContext(p.opt.Ctx, vars[ii]...); err != nil {
			return nil, fmt.Errorf("failed to execute %s error %w", p.SQL, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commiterror %w", err)
	}

	return []reflect.Value{}, nil
}

func (p *sqlParsed) createNamedVars(beanSize int, item0, bean reflect.Value) [][]interface{} {
	item := item0
	itemType := reflect.TypeOf(item0)
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
	if p.MaxSeq == 0 {
		fmt.Printf("start to exec %s: [%s]\n", p.ID, p.SQL)
	} else {
		fmt.Printf("start to exec %s: [%s] with args %v\n", p.ID, p.SQL, vars)
	}
}

func (p *sqlParsed) execBySeqRet1(db *sql.DB, outType reflect.Type, args []reflect.Value) ([]reflect.Value, error) {
	vars := p.makeVars(args)
	p.logPrepare(vars)

	result, err := db.ExecContext(p.opt.Ctx, p.SQL, vars...)
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
	isOutSlice := outType.Kind() == reflect.Slice
	outSlice := reflect.Value{}

	if isOutSlice {
		outSlice = reflect.MakeSlice(outType, 0, 0)
		outType = outType.Elem()
	}

	rows, err := p.doQuery(db, args) // nolint rowserrcheck
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns %s error %w", p.SQL, err)
	}

	interceptorFn := p.getRowScanInterceptorFn()
	maxRows := p.opt.QueryMaxRows
	mapFields := p.createMapFields(columns, outType)

	for ri := 0; rows.Next() && (maxRows <= 0 || ri < maxRows); ri++ {
		pointers, out := resetDests(outType, mapFields)
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan rows %s error %w", p.SQL, err)
		}

		fillFields(mapFields, out, pointers)

		if goon, err := interceptorFn(ri, out.Interface()); err != nil {
			return nil, err
		} else if !goon {
			break
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

func (p *sqlParsed) getRowScanInterceptorFn() RowScanInterceptorFn {
	if p.opt.RowScanInterceptor != nil {
		return p.opt.RowScanInterceptor.After
	}

	return func(rowIndex int, v interface{}) (bool, error) { return true, nil }
}

func (p *sqlParsed) doQuery(db *sql.DB, args []reflect.Value) (*sql.Rows, error) {
	vars := p.makeVars(args)

	p.logPrepare(vars)

	rows, err := db.QueryContext(p.opt.Ctx, p.SQL, vars...)
	if err != nil || rows.Err() != nil {
		if err == nil {
			err = rows.Err()
		}

		return nil, fmt.Errorf("execute %s error %w", p.SQL, err)
	}

	return rows, nil
}

func resetDests(outType reflect.Type, mapFields []*reflect.StructField) ([]interface{}, reflect.Value) {
	pointers := make([]interface{}, len(mapFields))
	out := reflect.Indirect(reflect.New(outType))

	for i, fv := range mapFields {
		if fv == nil {
			pointers[i] = &NullAny{Type: nil}
			continue
		}

		if ImplSQLScanner(fv.Type) {
			pointers[i] = reflect.New(fv.Type).Interface()
		} else {
			pointers[i] = &NullAny{Type: fv.Type}
		}
	}

	return pointers, out
}
func fillFields(mapFields []*reflect.StructField, out reflect.Value, pointers []interface{}) {
	for i, field := range mapFields {
		if field != nil {
			f := out.FieldByName(field.Name)

			if p, ok := pointers[i].(*NullAny); ok {
				f.Set(p.getVal())
			} else {
				f.Set(reflect.ValueOf(pointers[i]).Elem())
			}
		}
	}
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

func (p *sqlParsed) logError(err error) {
	fmt.Fprintf(os.Stderr, "%v\n", err)
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
