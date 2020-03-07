package sqlx

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/bingoohuang/goreflect"

	"github.com/bingoohuang/strcase"
)

// CreateDao fulfils the dao (should be pointer)
func CreateDao(driverName string, db *sql.DB, dao interface{}, createDaoOpts ...CreateDaoOpter) error {
	daov := reflect.ValueOf(dao)
	if daov.Kind() != reflect.Ptr || daov.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("dao should be pointer to struct")
	}

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

	v := reflect.Indirect(daov)
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

		r := sqlRun{DB: db, sqlParsed: p}

		if err := r.createFn(f, field, errSetter); err != nil {
			return err
		}
	}

	return nil
}

func (r *sqlRun) createFn(f reflect.StructField, v reflect.Value, errSetter errorSetter) error {
	numIn := f.Type.NumIn()
	numOut := f.Type.NumOut()

	lastOutError := numOut > 0 && goreflect.IsError(f.Type.Out(numOut-1)) // nolint gomnd
	if lastOutError {
		numOut--
	}

	var fn func([]reflect.Value) ([]reflect.Value, error)

	switch {
	case numIn == 0 && numOut == 0:
		fn = func([]reflect.Value) ([]reflect.Value, error) { return r.exec() }
	case numIn == 1 && r.isBindBy(byName) && numOut == 0:
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return r.execByNamedArg1Ret0(args[0]) }
	case r.isBindBy(bySeq, byAuto) && numOut == 0:
		fn = r.execBySeqArgsRet0
	case r.IsQuery && r.isBindBy(bySeq, byAuto, byNone) && numOut == 1:
		out := f.Type.Out(0)
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return r.queryBySeqRet1(out, args) }
	case !r.IsQuery && r.isBindBy(bySeq, byAuto) && numOut == 1:
		out := f.Type.Out(0)
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return r.execBySeqRet1(out, args) }
	default:
		err := fmt.Errorf("unsupportd func %v", f.Type)
		r.logError(err)

		return err
	}

	v.Set(reflect.MakeFunc(f.Type, func(args []reflect.Value) []reflect.Value {
		errSetter(nil)
		values, err := fn(args)
		if err != nil {
			errSetter(err)
			r.logError(err)

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

type sqlRun struct {
	*sql.DB
	*sqlParsed
}

func (r *sqlRun) exec() ([]reflect.Value, error) {
	r.logPrepare("(none)")
	_, err := r.ExecContext(r.opt.Ctx, r.SQL)

	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	return []reflect.Value{}, nil
}

func (r *sqlRun) execBySeqArgsRet0(args []reflect.Value) ([]reflect.Value, error) {
	vars := r.makeVars(args)
	r.logPrepare(vars)

	_, err := r.ExecContext(r.opt.Ctx, r.SQL, vars...)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	return []reflect.Value{}, nil
}

func (r *sqlRun) execByNamedArg1Ret0(bean reflect.Value) ([]reflect.Value, error) {
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

	tx, err := r.BeginTx(r.opt.Ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx %w", err)
	}

	pr, err := tx.PrepareContext(r.opt.Ctx, r.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare sql %s error %w", r.SQL, err)
	}

	vars := r.createNamedVars(itemSize, item0, bean)

	if isBeanSlice {
		r.logPrepare(vars)
	} else {
		r.logPrepare(vars[0])
	}

	for ii := 0; ii < itemSize; ii++ {
		if _, err := pr.ExecContext(r.opt.Ctx, vars[ii]...); err != nil {
			return nil, fmt.Errorf("failed to execute %s error %w", r.SQL, err)
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

func (r *sqlRun) execBySeqRet1(outType reflect.Type, args []reflect.Value) ([]reflect.Value, error) {
	vars := r.makeVars(args)
	r.logPrepare(vars)

	result, err := r.ExecContext(r.opt.Ctx, r.SQL, vars...)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	affected, err := convertRowsAffected(result, r.SQL, outType)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	return []reflect.Value{affected}, nil
}

func (r *sqlRun) queryBySeqRet1(outType reflect.Type, args []reflect.Value) ([]reflect.Value, error) {
	isOutSlice := outType.Kind() == reflect.Slice
	outSlice := reflect.Value{}

	if isOutSlice {
		outSlice = reflect.MakeSlice(outType, 0, 0)
		outType = outType.Elem()
	}

	rows, err := r.doQuery(args) // nolint rowserrcheck
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns %s error %w", r.SQL, err)
	}

	interceptorFn := r.getRowScanInterceptorFn()
	maxRows := r.opt.QueryMaxRows
	mapFields := r.createMapFields(columns, outType)

	for ri := 0; rows.Next() && (maxRows <= 0 || ri < maxRows); ri++ {
		pointers, out := resetDests(outType, mapFields)
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan rows %s error %w", r.SQL, err)
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

func (r *sqlRun) doQuery(args []reflect.Value) (*sql.Rows, error) {
	vars := r.makeVars(args)

	r.logPrepare(vars)

	rows, err := r.QueryContext(r.opt.Ctx, r.SQL, vars...)
	if err != nil || rows.Err() != nil {
		if err == nil {
			err = rows.Err()
		}

		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	return rows, nil
}

func (p *sqlParsed) createMapFields(columns []string, outType reflect.Type) []*reflect.StructField {
	mapFields := make([]*reflect.StructField, len(columns))

	for i, col := range columns {
		col := col
		fv, ok := outType.FieldByNameFunc(func(field string) bool {
			return matchesField2Col(outType, field, col)
		})

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

func matchesField2Col(structType reflect.Type, field, col string) bool {
	f, _ := structType.FieldByName(field)
	if tagName := f.Tag.Get("name"); tagName != "" {
		return tagName == col
	}

	return strings.EqualFold(field, col) || strings.EqualFold(field, strcase.ToCamel(col))
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
		if field == nil {
			continue
		}

		f := out.FieldByName(field.Name)

		if p, ok := pointers[i].(*NullAny); ok {
			f.Set(p.getVal())
		} else {
			f.Set(reflect.ValueOf(pointers[i]).Elem())
		}
	}
}
