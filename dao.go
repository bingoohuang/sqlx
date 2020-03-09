package sqlx

import (
	"database/sql"
	"fmt"
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

	sqlFilter := createSQLFilter(driverName)
	v := reflect.Indirect(daov)
	logger := createLogger(v, option)
	errSetter := createErrorSetter(v, option)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		f := v.Type().Field(i)

		if f.PkgPath != "" /* not exportable */ || f.Type.Kind() != reflect.Func {
			continue
		}

		sqlStmt := option.getSQLStmt(f)

		if sqlStmt == "" {
			return fmt.Errorf("failed to find sql with name %s", f.Name)
		}

		p, err := parseSQL(f.Name, sqlStmt)
		if err != nil {
			return fmt.Errorf("failed to parse sql %v error %w", sqlStmt, err)
		}

		p.opt = option
		p.logger = logger
		p.SQL = sqlFilter(p.SQL)
		numIn := f.Type.NumIn()

		if err := p.checkFuncInOut(numIn, sqlStmt, f); err != nil {
			return err
		}

		r := sqlRun{DB: db, sqlParsed: p, logger: logger}

		if err := r.createFn(f, field, errSetter); err != nil {
			return err
		}
	}

	return nil
}

func createSQLFilter(driverName string) func(s string) string {
	return func(s string) string {
		switch driverName {
		case "postgres":
			return replaceQuestionMark4Postgres(s)
		default:
			return s
		}
	}
}

func (option *CreateDaoOpt) getSQLStmt(f reflect.StructField) string {
	sqlStmt := f.Tag.Get("sql")
	if sqlStmt != "" {
		return sqlStmt
	}

	if option.DotSQL != nil {
		sqlName := f.Tag.Get("sqlName")
		if sqlName == "" {
			sqlName = f.Name
		}

		sqlStmt, _ = option.DotSQL.Raw(sqlName)
	}

	return sqlStmt
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
	case r.IsQuery && r.isBindBy(bySeq, byAuto, byNone) && numOut >= 1:
		outTyps := makeOutTypes(f.Type, numOut)
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return r.queryBySeqRet1(outTyps, args) }
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

func makeOutTypes(outType reflect.Type, numOut int) []reflect.Type {
	rt := make([]reflect.Type, numOut)

	for i := 0; i < numOut; i++ {
		rt[i] = outType.Out(i)
	}

	return rt
}

type sqlRun struct {
	*sql.DB
	*sqlParsed
	logger DaoLogger
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

	vars, err := r.createNamedVars(itemSize, item0, bean)
	if err != nil {
		return nil, err
	}

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

func (p *sqlParsed) createNamedVars(beanSize int, item0, bean reflect.Value) ([][]interface{}, error) {
	item := item0
	vars := make([][]interface{}, beanSize)
	itemType := item.Type()

	var namedValueParser func(name string, item reflect.Value, itemType reflect.Type) interface{}

	switch itemType.Kind() {
	case reflect.Struct:
		namedValueParser = func(name string, item reflect.Value, itemType reflect.Type) interface{} {
			return item.FieldByNameFunc(func(f string) bool {
				return matchesField2Col(itemType, f, name)
			}).Interface()
		}
	case reflect.Map:
		namedValueParser = func(name string, item reflect.Value, itemType reflect.Type) interface{} {
			return item.MapIndex(reflect.ValueOf(name)).Interface()
		}
	}

	if namedValueParser == nil {
		return nil, fmt.Errorf("unsupported type %v", itemType)
	}

	for ii := 0; ii < beanSize; ii++ {
		vars[ii] = make([]interface{}, len(p.Vars))

		if ii > 0 {
			item = bean.Index(ii)
		}

		for i, name := range p.Vars {
			vars[ii][i] = namedValueParser(name, item, itemType)
		}
	}

	return vars, nil
}

func (p *sqlParsed) logPrepare(vars interface{}) {
	p.logger.LogStart(p.ID, p.SQL, vars)
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

func (r *sqlRun) queryBySeqRet1(outTypes []reflect.Type, args []reflect.Value) ([]reflect.Value, error) {
	out0Type := outTypes[0]
	outSlice := reflect.Value{}

	if out0Type.Kind() == reflect.Slice {
		outSlice = reflect.MakeSlice(out0Type, 0, 0)
		out0Type = out0Type.Elem()
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
	mapFields, err := r.createMapFields(columns, out0Type, outTypes)

	if err != nil {
		return nil, err
	}

	for ri := 0; rows.Next() && (r.opt.QueryMaxRows <= 0 || ri < r.opt.QueryMaxRows); ri++ {
		pointers, out := resetDests(out0Type, outTypes, mapFields)
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan rows %s error %w", r.SQL, err)
		}

		fillFields(mapFields, pointers)

		if interceptorFn != nil {
			outValues := make([]interface{}, len(out))
			for i, outVal := range out {
				outValues[i] = outVal.Interface()
			}

			if goon, err := interceptorFn(ri, outValues...); err != nil {
				return nil, err
			} else if !goon {
				break
			}
		}

		if !outSlice.IsValid() {
			return out, nil
		}

		outSlice = reflect.Append(outSlice, out[0])
	}

	if outSlice.IsValid() {
		return []reflect.Value{outSlice}, nil
	}

	return r.noRows(out0Type, outTypes)
}

func (r *sqlRun) noRows(out0Type reflect.Type, outTypes []reflect.Type) ([]reflect.Value, error) {
	switch out0Type.Kind() {
	case reflect.Map:
		out := reflect.MakeMap(reflect.MapOf(out0Type.Key(), out0Type.Elem()))
		return []reflect.Value{out}, nil
	case reflect.Struct:
		return []reflect.Value{reflect.Indirect(reflect.New(out0Type))}, nil
	}

	outValues := make([]reflect.Value, len(outTypes))
	for i := range outTypes {
		outValues[i] = reflect.Indirect(reflect.New(outTypes[i]))
	}

	return outValues, nil
}

func (p *sqlParsed) getRowScanInterceptorFn() RowScanInterceptorFn {
	if p.opt.RowScanInterceptor != nil {
		return p.opt.RowScanInterceptor.After
	}

	return nil
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

func (p *sqlParsed) createMapFields(columns []string, out0Type reflect.Type,
	outTypes []reflect.Type) ([]selectItem, error) {
	switch out0Type.Kind() {
	case reflect.Struct, reflect.Map:
		if len(outTypes) != 1 { // nolint gomnd
			return nil, fmt.Errorf("unsupported return type  %v for current sql %v", out0Type, p.SQL)
		}
	}

	switch out0Type.Kind() {
	case reflect.Struct:
		mapFields := make([]selectItem, len(columns))
		for i, col := range columns {
			mapFields[i] = p.makeStructField(col, out0Type)
		}

		return mapFields, nil
	case reflect.Map:
		mapFields := make([]selectItem, len(columns))
		for i, col := range columns {
			mapFields[i] = p.makeMapField(col, out0Type)
		}

		return mapFields, nil
	}

	mapFields := make([]selectItem, len(columns))

	for i := range columns {
		if i < len(outTypes) {
			mapFields[i] = &singleValue{
				vType: outTypes[i],
			}
		} else {
			mapFields[i] = &singleValue{
				vType: reflect.TypeOf(""),
			}
		}
	}

	return mapFields, nil
}

func (p *sqlParsed) makeMapField(col string, outType reflect.Type) selectItem {
	return &mapItem{
		k:     reflect.ValueOf(col),
		vType: outType.Elem(),
	}
}

func (p *sqlParsed) makeStructField(col string, outType reflect.Type) selectItem {
	fv, ok := outType.FieldByNameFunc(func(field string) bool {
		return matchesField2Col(outType, field, col)
	})

	if ok {
		return &structItem{StructField: &fv}
	}

	return nil
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

func (p *sqlParsed) logError(err error) { p.logger.LogError(err) }

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

type selectItem interface {
	Type() reflect.Type
	Set(val reflect.Value)
	ResetParent(parent reflect.Value)
}

type structItem struct {
	*reflect.StructField
	parent reflect.Value
}

func (s *structItem) Type() reflect.Type               { return s.StructField.Type }
func (s *structItem) ResetParent(parent reflect.Value) { s.parent = parent }
func (s *structItem) Set(val reflect.Value) {
	f := s.parent.FieldByName(s.StructField.Name)
	f.Set(val)
}

type mapItem struct {
	k      reflect.Value
	vType  reflect.Type
	parent reflect.Value
}

func (s *mapItem) Type() reflect.Type               { return s.vType }
func (s *mapItem) ResetParent(parent reflect.Value) { s.parent = parent }
func (s *mapItem) Set(val reflect.Value)            { s.parent.SetMapIndex(s.k, val) }

type singleValue struct {
	parent reflect.Value
	vType  reflect.Type
}

func (s *singleValue) Type() reflect.Type               { return s.vType }
func (s *singleValue) ResetParent(parent reflect.Value) { s.parent = parent }
func (s *singleValue) Set(val reflect.Value)            { s.parent.Set(val) }

func resetDests(out0Type reflect.Type, outTypes []reflect.Type,
	mapFields []selectItem) ([]interface{}, []reflect.Value) {
	pointers := make([]interface{}, len(mapFields))

	var out0 reflect.Value

	hasParent := false
	out := make([]reflect.Value, len(outTypes))

	switch out0Type.Kind() {
	case reflect.Map:
		hasParent = true
		out0 = reflect.MakeMap(reflect.MapOf(out0Type.Key(), out0Type.Elem()))
		out[0] = out0
	case reflect.Struct:
		hasParent = true
		out0 = reflect.Indirect(reflect.New(out0Type))
		out[0] = out0
	}

	for i, fv := range mapFields {
		if fv == nil {
			pointers[i] = &NullAny{Type: nil}
			continue
		}

		if hasParent {
			fv.ResetParent(out0)
		} else if i < len(outTypes) {
			out[i] = reflect.Indirect(reflect.New(outTypes[i]))
			fv.ResetParent(out[i])
		}

		if ImplSQLScanner(fv.Type()) {
			pointers[i] = reflect.New(fv.Type()).Interface()
		} else {
			pointers[i] = &NullAny{Type: fv.Type()}
		}
	}

	return pointers, out
}

func fillFields(mapFields []selectItem, pointers []interface{}) {
	for i, field := range mapFields {
		if field == nil {
			continue
		}

		if p, ok := pointers[i].(*NullAny); ok {
			field.Set(p.getVal())
		} else {
			field.Set(reflect.ValueOf(pointers[i]).Elem())
		}
	}
}
