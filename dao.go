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
func CreateDao(db *sql.DB, dao interface{}, createDaoOpts ...CreateDaoOpter) error {
	daov := reflect.ValueOf(dao)
	if daov.Kind() != reflect.Ptr || daov.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("dao should be pointer to struct")
	}

	option, err := applyCreateDaoOption(createDaoOpts)
	if err != nil {
		return err
	}

	driverName := LookupDriverName(db.Driver())
	sqlFilter := createSQLFilter(driverName)
	v := reflect.Indirect(daov)
	createLogger(v, option)
	createErrorSetter(v, option)

	structValue := MakeStructValue(v)
	for i := 0; i < structValue.NumField; i++ {
		f := structValue.FieldByIndex(i)

		if f.PkgPath != "" /* not exportable */ || f.Kind != reflect.Func {
			continue
		}

		tags, err := ParseTags(string(f.Tag))
		if err != nil {
			return err
		}

		sqlStmt, sqlName := option.getSQLStmt(f, tags, 0)
		if sqlStmt == nil {
			return fmt.Errorf("failed to find sqlName %s", f.Name)
		}

		parsed, err := parseSQL(sqlName, sqlStmt.Raw())
		if err != nil {
			return err
		}

		p := &sqlParsed{
			ID:  sqlName,
			SQL: sqlStmt,
		}

		p.opt = option
		p.sqlFilter = sqlFilter

		p.BindBy = parsed.BindBy
		p.IsQuery = parsed.IsQuery

		r := sqlRun{DB: db, sqlParsed: p}

		if err := r.createFn(f); err != nil {
			return err
		}
	}

	return nil
}

// MapValueOrDefault returns the value associated to the key,
// or return defaultValue when value does not exits or it is empty.
func MapValueOrDefault(m map[string]string, key, defaultValue string) string {
	if v, ok := m[key]; ok && v != "" {
		return v
	}

	return defaultValue
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

func (option *CreateDaoOpt) getSQLStmt(field StructField, tags Tags, stack int) (SQLPart, string) {
	if stack > 10 { // nolint gomnd
		return nil, ""
	}

	if sqlStmt := field.GetTag("sql"); sqlStmt != "" {
		dsi := DotSQLItem{
			Name:    field.Name,
			Content: []string{sqlStmt},
			Attrs:   tags.Map(),
		}
		part, err := dsi.DynamicSQL()

		if err != nil {
			option.Logger.LogError(err)
		}

		return part, field.Name
	}

	sqlName := field.GetTagOr("sqlName", field.Name)
	if part, err := option.DotSQL(sqlName); err != nil {
		option.Logger.LogError(err)
	} else if part != nil {
		return part, sqlName
	}

	if sqlName == field.Name {
		return nil, ""
	}

	if field, ok := field.Parent.FieldByName(sqlName); ok {
		return option.getSQLStmt(field, nil, stack+1) // nolint gomnd
	}

	return nil, sqlName
}

func (r *sqlRun) createFn(f StructField) error {
	numIn := f.Type.NumIn()
	numOut := f.Type.NumOut()

	lastOutError := numOut > 0 && goreflect.IsError(f.Type.Out(numOut-1)) // nolint gomnd
	if lastOutError {
		numOut--
	}

	var fn func([]reflect.Value) ([]reflect.Value, error)

	switch {
	case numIn == 0 && numOut == 0:
		fn = func([]reflect.Value) ([]reflect.Value, error) {
			return r.exec(numIn, f)
		}
	case numIn == 1 && r.isBindBy(byName) && numOut == 0:
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return r.execByNamedArg1Ret0(numIn, f, args[0]) }
	case r.isBindBy(bySeq, byAuto) && numOut == 0:
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return r.execBySeqArgsRet0(numIn, f, args) }
	case r.IsQuery && r.isBindBy(bySeq, byAuto, byNone) && numOut >= 1:
		outTypes := makeOutTypes(f.Type, numOut)
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return r.queryBySeqRet1(numIn, f, outTypes, args) }
	case numIn == 1 && r.IsQuery && r.isBindBy(byName) && numOut >= 1:
		outTypes := makeOutTypes(f.Type, numOut)
		fn = func(args []reflect.Value) ([]reflect.Value, error) {
			return r.queryByNameRet1(numIn, f, args[0], outTypes)
		}
	case !r.IsQuery && r.isBindBy(bySeq, byAuto) && numOut == 1:
		out := f.Type.Out(0)
		fn = func(args []reflect.Value) ([]reflect.Value, error) { return r.execBySeqRet1(numIn, f, out, args) }
	default:
		err := fmt.Errorf("unsupportd func %s %v", f.Name, f.Type)
		r.logError(err)

		return err
	}

	f.Field.Set(reflect.MakeFunc(f.Type, func(args []reflect.Value) []reflect.Value {
		r.opt.ErrSetter(nil)
		values, err := fn(args)
		if err != nil {
			r.opt.ErrSetter(err)
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
}

func (r *sqlRun) exec(numIn int, f StructField) ([]reflect.Value, error) {
	runSQL, err := r.evalSeq(numIn, f, nil)
	r.logPrepare(runSQL, "(none)")

	if err != nil {
		return nil, err
	}

	_, err = r.ExecContext(r.opt.Ctx, runSQL)

	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	return []reflect.Value{}, nil
}

func (r *sqlRun) execBySeqArgsRet0(numIn int, f StructField, args []reflect.Value) ([]reflect.Value, error) {
	runSQL, err := r.evalSeq(numIn, f, args)
	if err != nil {
		return nil, err
	}

	vars := r.makeVars(args)
	r.logPrepare(runSQL, vars)

	_, err = r.ExecContext(r.opt.Ctx, runSQL, vars...)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	return []reflect.Value{}, nil
}

func (r *sqlRun) evalSeq(numIn int, f StructField, args []reflect.Value) (string, error) {
	env := make(map[string]interface{})
	for i, arg := range args {
		env[fmt.Sprintf("_%d", i+1)] = arg.Interface() // nolint gomnd
	}

	return r.eval(numIn, f, env)
}

func (r *sqlRun) eval(numIn int, f StructField, env map[string]interface{}) (string, error) {
	runSQL, err := r.SQL.Eval(env)
	if err != nil {
		return "", err
	}

	if err := r.parseSQL(r.ID, runSQL); err != nil {
		return "", err
	}

	if err := r.checkFuncInOut(numIn, f); err != nil {
		return "", err
	}

	return runSQL, nil
}

func (r *sqlRun) queryByNameRet1(numIn int, f StructField, bean reflect.Value,
	outTypes []reflect.Type) ([]reflect.Value, error) {
	env := r.createNamedMap(bean)

	runSQL, err := r.eval(numIn, f, env)
	if err != nil {
		return nil, err
	}

	parsed, err := parseSQL(r.ID, runSQL)
	if err != nil {
		return nil, err
	}

	r.resetSQLParsed(parsed)

	vars, err := r.createNamedVars(bean)
	if err != nil {
		return nil, err
	}

	rows, err := r.doQueryDirectVars(runSQL, vars) // nolint rowserrcheck
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return r.processQueryRows(rows, outTypes)
}

// nolint funlen
func (r *sqlRun) execByNamedArg1Ret0(numIn int, f StructField, bean reflect.Value) ([]reflect.Value, error) {
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

	var (
		err error
		pr  *sql.Stmt
	)

	tx, err := r.BeginTx(r.opt.Ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx %w", err)
	}

	lastSQL := ""

	for ii := 0; ii < itemSize; ii++ {
		if ii > 0 {
			item0 = bean.Index(ii)
		}

		runSQL, err := r.eval(numIn, f, r.createNamedMap(item0))

		if err != nil {
			return nil, err
		}

		if lastSQL != runSQL {
			lastSQL = runSQL

			parsed, err := parseSQL(r.ID, runSQL)
			if err != nil {
				return nil, err
			}

			r.resetSQLParsed(parsed)

			pr, err = tx.PrepareContext(r.opt.Ctx, r.Stmt)
			if err != nil {
				return nil, fmt.Errorf("failed to prepare sql %s error %w", r.SQL, err)
			}
		}

		vars, err := r.createNamedVars(item0)
		if err != nil {
			return nil, err
		}

		if isBeanSlice {
			r.logPrepare(runSQL, vars)
		} else {
			r.logPrepare(runSQL, vars[0])
		}

		if _, err := pr.ExecContext(r.opt.Ctx, vars...); err != nil {
			return nil, fmt.Errorf("failed to execute %s error %w", r.SQL, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commiterror %w", err)
	}

	return []reflect.Value{}, nil
}

func (p *sqlParsed) createNamedMap(bean reflect.Value) map[string]interface{} {
	m := make(map[string]interface{})

	switch bean.Type().Kind() {
	case reflect.Struct:
		structValue := MakeStructValue(bean)
		for i, f := range structValue.FieldTypes {
			name := f.Name
			if tagName := f.Tag.Get("name"); tagName != "" {
				name = tagName
			} else {
				name = strcase.ToCamelLower(name)
			}

			m[name] = bean.Field(i).Interface()
		}
	case reflect.Map:
		for _, k := range bean.MapKeys() {
			m[k.Interface().(string)] = bean.MapIndex(k).Interface()
		}
	}

	return m
}

func (p *sqlParsed) createNamedVars(bean reflect.Value) ([]interface{}, error) {
	itemType := bean.Type()

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

	vars := make([]interface{}, len(p.Vars))

	for i, name := range p.Vars {
		vars[i] = namedValueParser(name, bean, itemType)
	}

	return vars, nil
}

func (p *sqlParsed) logPrepare(runSQL string, vars interface{}) {
	p.opt.Logger.LogStart(p.ID, runSQL, vars)
}

func (r *sqlRun) execBySeqRet1(numIn int, f StructField,
	outType reflect.Type, args []reflect.Value) ([]reflect.Value, error) {
	runSQL, err := r.evalSeq(numIn, f, args)
	if err != nil {
		return nil, err
	}

	vars := r.makeVars(args)
	r.logPrepare(runSQL, vars)

	result, err := r.ExecContext(r.opt.Ctx, runSQL, vars...)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	affected, err := convertRowsAffected(result, runSQL, outType)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	return []reflect.Value{affected}, nil
}

func (r *sqlRun) queryBySeqRet1(numIn int, f StructField,
	outTypes []reflect.Type, args []reflect.Value) ([]reflect.Value, error) {
	runSQL, err := r.evalSeq(numIn, f, args)
	if err != nil {
		return nil, err
	}

	parsed, err := parseSQL(r.ID, runSQL)
	if err != nil {
		return nil, err
	}

	r.resetSQLParsed(parsed)

	rows, err := r.doQuery(r.Stmt, args) // nolint rowserrcheck
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return r.processQueryRows(rows, outTypes)
}

func (r *sqlRun) processQueryRows(rows *sql.Rows, outTypes []reflect.Type) ([]reflect.Value, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns %s error %w", r.SQL, err)
	}

	out0Type := outTypes[0]
	outSlice := reflect.Value{}
	out0TypePtr := out0Type.Kind() == reflect.Ptr

	switch out0Type.Kind() {
	case reflect.Slice:
		outSlice = reflect.MakeSlice(out0Type, 0, 0)
		out0Type = out0Type.Elem()
	case reflect.Ptr:
		out0Type = out0Type.Elem()
	}

	interceptorFn := r.getRowScanInterceptorFn()
	mapFields, err := r.createMapFields(columns, out0Type, outTypes)

	if err != nil {
		return nil, err
	}

	for ri := 0; rows.Next() && (r.opt.QueryMaxRows <= 0 || ri < r.opt.QueryMaxRows); ri++ {
		pointers, out := resetDests(out0Type, out0TypePtr, outTypes, mapFields)
		if err := rows.Scan(pointers[:len(columns)]...); err != nil {
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
			return out[:len(outTypes)], nil
		}

		outSlice = reflect.Append(outSlice, out[0])
	}

	if outSlice.IsValid() {
		return []reflect.Value{outSlice}, nil
	}

	return r.noRows(out0Type, out0TypePtr, outTypes)
}

func (r *sqlRun) noRows(out0Type reflect.Type, out0TypePtr bool, outTypes []reflect.Type) ([]reflect.Value, error) {
	switch out0Type.Kind() {
	case reflect.Map:
		out := reflect.MakeMap(reflect.MapOf(out0Type.Key(), out0Type.Elem()))
		return []reflect.Value{out}, nil
	case reflect.Struct:
		if out0TypePtr {
			return []reflect.Value{reflect.Zero(outTypes[0])}, nil
		}

		return []reflect.Value{reflect.Indirect(reflect.New(out0Type))}, nil
	}

	outValues := make([]reflect.Value, len(outTypes))
	for i := range outTypes {
		outValues[i] = reflect.Indirect(reflect.New(outTypes[i]))
	}

	return outValues, sql.ErrNoRows
}

func (p *sqlParsed) getRowScanInterceptorFn() RowScanInterceptorFn {
	if p.opt.RowScanInterceptor != nil {
		return p.opt.RowScanInterceptor.After
	}

	return nil
}

func (r *sqlRun) doQuery(runSQL string, args []reflect.Value) (*sql.Rows, error) {
	vars := r.makeVars(args)

	return r.doQueryDirectVars(runSQL, vars)
}

func (r *sqlRun) doQueryDirectVars(runSQL string, vars []interface{}) (*sql.Rows, error) {
	r.logPrepare(runSQL, vars)

	rows, err := r.QueryContext(r.opt.Ctx, runSQL, vars...)
	if err != nil || rows.Err() != nil {
		if err == nil {
			err = rows.Err()
		}

		return nil, fmt.Errorf("execute %s error %w", runSQL, err)
	}

	return rows, nil
}

func (r *sqlRun) resetSQLParsed(p *sqlParsed) {
	r.Stmt = p.Stmt
	r.IsQuery = p.IsQuery
	r.Vars = p.Vars
	r.MaxSeq = p.MaxSeq
	r.BindBy = p.BindBy
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

	mapFields := make([]selectItem, max(len(columns), len(outTypes)))

	for i := range columns {
		if i < len(outTypes) {
			vType := outTypes[i]
			ptr := vType.Kind() == reflect.Ptr

			if ptr {
				vType = vType.Elem()
			}

			mapFields[i] = &singleValue{vType: vType, ptr: ptr}
		} else {
			mapFields[i] = &singleValue{vType: reflect.TypeOf("")}
		}
	}

	for i := len(columns); i < len(outTypes); i++ {
		mapFields[i] = &singleValue{vType: outTypes[i]}
	}

	return mapFields, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}

func (p *sqlParsed) makeMapField(col string, outType reflect.Type) selectItem {
	return &mapItem{k: reflect.ValueOf(col), vType: outType.Elem()}
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

func (p *sqlParsed) logError(err error) { p.opt.Logger.LogError(err) }

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
	ptr    bool
	parent reflect.Value
	vType  reflect.Type
}

func (s *singleValue) Type() reflect.Type               { return s.vType }
func (s *singleValue) ResetParent(parent reflect.Value) { s.parent = parent }
func (s *singleValue) Set(val reflect.Value) {
	if !s.parent.IsValid() {
		s.parent = reflect.Indirect(reflect.New(s.vType))
	}

	s.parent.Set(val)
}

func resetDests(out0Type reflect.Type, out0TypePtr bool,
	outTypes []reflect.Type, mapFields []selectItem) ([]interface{}, []reflect.Value) {
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
		out0Ptr := reflect.New(out0Type)
		out0 = reflect.Indirect(out0Ptr)

		if out0TypePtr {
			out[0] = out0Ptr
		} else {
			out[0] = out0
		}
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
