package sqlx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/bingoohuang/sqlparser/sqlparser"

	"github.com/bingoohuang/gor"
	"github.com/bingoohuang/strcase"
)

type Limit struct {
	Offset int64
	Length int64
}

type Count int64

var (
	LimitType = reflect.TypeOf((*Limit)(nil)).Elem()
	CountType = reflect.TypeOf((*Count)(nil)).Elem()
)

// GetDBFn is the function type to get a sql.DBGetter.
type GetDBFn func() SqlDB

// DBGetter is the interface to get a sql.DBGetter.
type DBGetter interface{ GetDB() SqlDB }

// GetDB returns a sql.DBGetter.
func (f GetDBFn) GetDB() SqlDB { return f() }

// StdDB is the wrapper for sql.DBGetter.
type StdDB struct{ db SqlDB }

// MakeDB makes a new StdDB from sql.DBGetter.
func MakeDB(db SqlDB) *StdDB { return &StdDB{db: db} }

// GetDB returns a sql.DBGetter.
func (f StdDB) GetDB() SqlDB { return f.db }

type SqlDB interface {
	Driver() driver.Driver

	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// nolint:gochecknoglobals
var (
	// DB is the global sql.DB for convenience.
	DB SqlDB
)

// CreateDao fulfils the dao (should be pointer).
func CreateDao(dao interface{}, createDaoOpts ...CreateDaoOpter) error {
	daov := reflect.ValueOf(dao)
	if daov.Kind() != reflect.Ptr || daov.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("dao should be pointer to struct") // nolint:goerr113
	}

	option, err := applyCreateDaoOption(createDaoOpts)
	if err != nil {
		return err
	}

	v := reflect.Indirect(daov)
	createDBGetter(v, option)
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
			return fmt.Errorf("failed to find sqlName %s", f.Name) // nolint:goerr113
		}

		parsed := &SQLParsed{
			ID:  sqlName,
			SQL: sqlStmt,
			opt: option,
		}

		if err := parsed.fastParseSQL(sqlStmt.Raw()); err != nil {
			return err
		}

		r := sqlRun{SQLParsed: parsed}
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

func (option *CreateDaoOpt) getSQLStmt(field StructField, tags Tags, stack int) (SQLPart, string) {
	if stack > 10 {
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
		return option.getSQLStmt(field, nil, stack+1)
	}

	return nil, sqlName
}

func (r *sqlRun) createFn(f StructField) error {
	numIn := f.Type.NumIn()
	numOut := f.Type.NumOut()

	lastOutError := numOut > 0 && gor.IsError(f.Type.Out(numOut-1))
	if lastOutError {
		numOut--
	}

	fn := r.MakeFunc(f, numIn, numOut)
	if fn == nil {
		err := fmt.Errorf("unsupportd func %s %v", f.Name, f.Type) // nolint:goerr113
		r.logError(err)

		return err
	}

	f.Field.Set(reflect.MakeFunc(f.Type, func(args []reflect.Value) []reflect.Value {
		r.opt.ErrSetter(nil)
		values, err := fn(args)
		if err != nil {
			r.opt.ErrSetter(err)
			r.logError(err)

			values = make([]reflect.Value, numOut, numOut+1)
			for i := 0; i < numOut; i++ {
				values[i] = reflect.Zero(f.Type.Out(i))
			}
		}

		if lastOutError {
			if err != nil {
				values = append(values, reflect.ValueOf(err))
			} else {
				values = append(values, reflect.Zero(gor.ErrType))
			}
		}

		return values
	}))

	return nil
}

func (r *sqlRun) MakeFunc(f StructField, numIn, numOut int) func([]reflect.Value) ([]reflect.Value, error) {
	var fn func(int, StructField, []reflect.Type, []reflect.Value) ([]reflect.Value, error)

	switch isBindByName := r.isBindBy(ByName); {
	case !r.IsQuery && isBindByName:
		fn = r.execByName
	case !r.IsQuery && !isBindByName:
		fn = r.execBySeq
	case r.IsQuery && isBindByName:
		fn = r.queryByName
	default: // isQuery && !isBindByName:
		fn = r.queryBySeq
	}

	return func(args []reflect.Value) ([]reflect.Value, error) {
		return fn(numIn, f, makeOutTypes(f.Type, numOut), args)
	}
}

func makeOutTypes(outType reflect.Type, numOut int) []reflect.Type {
	rt := make([]reflect.Type, numOut)

	for i := 0; i < numOut; i++ {
		rt[i] = outType.Out(i)
	}

	return rt
}

type sqlRun struct {
	*SQLParsed
}

func (p *SQLParsed) evalSeq(numIn int, f StructField, args []reflect.Value) error {
	env := make(map[string]interface{})
	for i, arg := range args {
		env[fmt.Sprintf("_%d", i+1)] = arg.Interface()
	}

	if len(args) > 0 {
		env = p.createFieldSqlParts(env, args[0])
	}

	return p.eval(numIn, f, env)
}

func (p *SQLParsed) eval(numIn int, f StructField, env map[string]interface{}) error {
	runSQL, err := p.SQL.Eval(env)
	if err != nil {
		return err
	}

	if err := p.parseSQL(runSQL); err != nil {
		return err
	}

	if err := p.checkFuncInOut(numIn, f); err != nil {
		return err
	}

	return nil
}

func (r *sqlRun) queryByName(numIn int, f StructField,
	outTypes []reflect.Type, args []reflect.Value) ([]reflect.Value, error) {
	var bean reflect.Value

	if numIn > 0 {
		bean = args[0]
	}

	parsed := *r.SQLParsed
	env := parsed.createNamedMap(bean)

	if err := parsed.eval(numIn, f, env); err != nil {
		return nil, err
	}

	vars, err := parsed.createNamedVars(bean)
	if err != nil {
		return nil, err
	}

	counterIndex := indexOfTypes(outTypes, CountType)
	db := r.opt.DBGetter.GetDB()
	rows, counter, err := parsed.doQueryDirectVars(db, vars, counterIndex >= 0)
	if err != nil {
		return nil, err
	}

	return parsed.wrapCounter(rows, outTypes, counterIndex, counter)
}

func (p *SQLParsed) wrapCounter(rows *sql.Rows, outTypes []reflect.Type, counterIndex int, counterFn func() (int64, error)) ([]reflect.Value, error) {
	values, err := p.processQueryRows(rows, remove(outTypes, counterIndex))
	_ = rows.Close()
	if err != nil || counterFn == nil {
		return values, err
	}

	counter, err := counterFn()
	if err != nil {
		return values, err
	}

	return insert(values, counterIndex, reflect.ValueOf(Count(counter))), nil
}

func indexOfTypes(types []reflect.Type, typ reflect.Type) int {
	for i, t := range types {
		if t == typ {
			return i
		}
	}

	return -1
}

// nolint:funlen
func (r *sqlRun) execByName(numIn int, f StructField, outTypes []reflect.Type,
	args []reflect.Value) ([]reflect.Value, error) {
	var bean reflect.Value

	if numIn > 0 {
		bean = args[0]
	}

	item0 := bean
	itemSize := 1
	isBeanSlice := bean.IsValid() && bean.Type().Kind() == reflect.Slice

	if isBeanSlice {
		if bean.IsNil() || bean.Len() == 0 {
			return []reflect.Value{}, nil
		}

		item0 = bean.Index(0)
		itemSize = bean.Len()
	}

	var (
		err        error
		pr         *sql.Stmt
		lastResult sql.Result
		lastSQL    string
	)

	parsed := *r.SQLParsed
	db := r.opt.DBGetter.GetDB()
	tx, err := db.BeginTx(parsed.opt.Ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx %w", err)
	}

	for ii := 0; ii < itemSize; ii++ {
		if ii > 0 {
			item0 = bean.Index(ii)
		}

		namedMap := parsed.createNamedMap(item0)
		if err := parsed.eval(numIn, f, namedMap); err != nil {
			return nil, err
		}

		if lastSQL != parsed.runSQL {
			lastSQL = parsed.runSQL

			if pr, err = tx.PrepareContext(parsed.opt.Ctx, parsed.runSQL); err != nil {
				return nil, fmt.Errorf("failed to prepare sql %s error %w", r.RawStmt, err)
			}
		}

		vars, err := parsed.createNamedVars(item0)
		if err != nil {
			return nil, err
		}

		parsed.logPrepare(vars)

		lastResult, err = pr.ExecContext(parsed.opt.Ctx, vars...)

		if err != nil {
			return nil, fmt.Errorf("failed to execute %s error %w", parsed.runSQL, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commiterror %w", err)
	}

	return convertExecResult(lastResult, lastSQL, outTypes)
}

func (p *SQLParsed) createFieldSqlParts(m map[string]interface{}, bean reflect.Value) map[string]interface{} {
	if !bean.IsValid() || bean.Type().Kind() != reflect.Struct {
		return m
	}

	structValue := MakeStructValue(bean)
	for i, f := range structValue.FieldTypes {
		if sqlPart := f.Tag.Get("sql"); sqlPart != "" {
			if bean.Field(i).IsZero() {
				continue
			}

			if f.Type.AssignableTo(LimitType) {
				l := bean.Field(i).Interface().(Limit)
				p.fp.AddFieldSqlPart(sqlPart,
					[]interface{}{l.Offset, l.Length}, false)
			} else {
				p.fp.AddFieldSqlPart(sqlPart,
					[]interface{}{bean.Field(i).Interface()}, true)
			}
		}
	}

	return m
}

func (p *SQLParsed) createNamedMap(bean reflect.Value) map[string]interface{} {
	m := make(map[string]interface{})
	if !bean.IsValid() {
		return m
	}

	switch bean.Type().Kind() {
	case reflect.Struct:
		structValue := MakeStructValue(bean)
		for i, f := range structValue.FieldTypes {
			if tagName := f.Tag.Get("name"); tagName != "" {
				m[tagName] = bean.Field(i).Interface()
			} else {
				name := strcase.ToCamelLower(f.Name)
				m[name] = bean.Field(i).Interface()
			}

		}
	case reflect.Map:
		for _, k := range bean.MapKeys() {
			if ks, ok := k.Interface().(string); ok {
				m[ks] = bean.MapIndex(k).Interface()
			}
		}
	}

	return m
}

func (p *SQLParsed) createNamedVars(bean reflect.Value) ([]interface{}, error) {
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
		// nolint:goerr113
		return nil, fmt.Errorf("named vars should use struct/map, unsupported type %v", itemType)
	}

	vars := make([]interface{}, len(p.Vars))

	for i, name := range p.Vars {
		vars[i] = namedValueParser(name, bean, itemType)
	}

	return vars, nil
}

func (p *SQLParsed) logPrepare(vars interface{}) {
	p.opt.Logger.LogStart(p.ID, p.runSQL, vars)
}

func (r *sqlRun) execBySeq(numIn int, f StructField,
	outTypes []reflect.Type, args []reflect.Value) ([]reflect.Value, error) {
	parsed := *r.SQLParsed

	if err := parsed.evalSeq(numIn, f, args); err != nil {
		return nil, err
	}

	vars := parsed.makeVars(args)
	parsed.logPrepare(vars)

	db := r.opt.DBGetter.GetDB()
	result, err := db.ExecContext(parsed.opt.Ctx, parsed.runSQL, vars...)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	results, err := convertExecResult(result, parsed.runSQL, outTypes)
	if err != nil {
		return nil, fmt.Errorf("execute %s error %w", r.SQL, err)
	}

	return results, nil
}

func (r *sqlRun) queryBySeq(numIn int, f StructField,
	outTypes []reflect.Type, args []reflect.Value) ([]reflect.Value, error) {

	parsed := *r.SQLParsed
	if err := parsed.evalSeq(numIn, f, args); err != nil {
		return nil, err
	}

	db := r.opt.DBGetter.GetDB()
	counterIndex := indexOfTypes(outTypes, CountType)

	rows, counterFn, err := parsed.doQuery(db, args, counterIndex >= 0)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return parsed.wrapCounter(rows, outTypes, counterIndex, counterFn)
}

func (p *SQLParsed) processQueryRows(rows *sql.Rows, outTypes []reflect.Type) ([]reflect.Value, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns %s error %w", p.SQL, err)
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

	interceptorFn := p.getRowScanInterceptorFn()
	mapFields, err := p.createMapFields(columns, out0Type, outTypes)

	if err != nil {
		return nil, err
	}

	for ri := 0; rows.Next() && (p.opt.QueryMaxRows <= 0 || ri < p.opt.QueryMaxRows); ri++ {
		pointers, out := resetDests(out0Type, out0TypePtr, outTypes, mapFields)
		if err := rows.Scan(pointers[:len(columns)]...); err != nil {
			return nil, fmt.Errorf("scan rows %s error %w", p.SQL, err)
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

	return noRows(out0Type, out0TypePtr, outTypes)
}

func noRows(out0Type reflect.Type, out0TypePtr bool, outTypes []reflect.Type) ([]reflect.Value, error) {
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

func (p *SQLParsed) getRowScanInterceptorFn() RowScanInterceptorFn {
	if p.opt.RowScanInterceptor != nil {
		return p.opt.RowScanInterceptor.After
	}

	return nil
}

func (p *SQLParsed) doQuery(db SqlDB, args []reflect.Value, counting bool) (*sql.Rows, func() (int64, error), error) {
	vars := p.makeVars(args)
	return p.doQueryDirectVars(db, vars, counting)
}

func (p *SQLParsed) doQueryDirectVars(db SqlDB, vars []interface{}, counting bool) (*sql.Rows, func() (int64, error), error) {
	p.logPrepare(vars)

	query := p.runSQL
	rows, err := db.QueryContext(p.opt.Ctx, query, vars...)
	if err != nil || rows.Err() != nil {
		if err == nil {
			err = rows.Err()
		}

		return nil, nil, fmt.Errorf("execute %s error %w", query, err)
	}

	if counting {
		return rows, func() (int64, error) {
			count, err := p.pagingCount(db, query, vars)
			return count, err
		}, nil
	}

	return rows, nil, nil
}

func (p *SQLParsed) pagingCount(db SqlDB, query string, vars []interface{}) (int64, error) {
	parsed, err := sqlparser.Parse(query)
	if err != nil {
		return 0, err
	}

	selectQuery, ok := parsed.(*sqlparser.Select)
	if !ok {
		return 0, errors.New("not select query")
	}

	selectQuery.SelectExprs = countStarExprs
	selectQuery.OrderBy = nil
	selectQuery.Having = nil
	oldLimit := selectQuery.Limit
	selectQuery.Limit = nil

	limitVarsCount := 0
	if oldLimit != nil {
		limitVarsCount++
		if oldLimit.Offset != nil {
			limitVarsCount++
		}
	}

	countQuery := sqlparser.String(selectQuery)
	vars = vars[:len(vars)-limitVarsCount]

	log.Printf("I! execute qury %s with args %v", countQuery, vars)
	rows, err := db.QueryContext(p.opt.Ctx, countQuery, vars...)
	if err != nil || rows.Err() != nil {
		if err == nil {
			err = rows.Err()
		}

		return 0, fmt.Errorf("execute %s error %w", countQuery, err)
	}

	defer rows.Close()

	rows.Next()
	var count int64
	if err := rows.Scan(&count); err != nil {
		return 0, err
	}

	return count, nil

}

func (p *SQLParsed) createMapFields(columns []string, out0Type reflect.Type,
	outTypes []reflect.Type) ([]selectItem, error) {
	switch out0Type.Kind() {
	case reflect.Struct, reflect.Map:
		if len(outTypes) != 1 {
			// nolint:goerr113
			return nil, fmt.Errorf("unsupported return type  %v for current sql %v", out0Type, p.SQL)
		}
	}

	lenCol := len(columns)
	switch out0Type.Kind() {
	case reflect.Struct:
		mapFields := make([]selectItem, lenCol)
		for i, col := range columns {
			mapFields[i] = p.makeStructField(col, out0Type)
		}

		return mapFields, nil
	case reflect.Map:
		mapFields := make([]selectItem, lenCol)
		for i, col := range columns {
			mapFields[i] = p.makeMapField(col, out0Type)
		}

		return mapFields, nil
	}

	mapFields := make([]selectItem, max(lenCol, len(outTypes)))

	for i := range columns {
		if i < len(outTypes) {
			vType := out0Type
			if i > 0 {
				vType = outTypes[i]
			}

			ptr := vType.Kind() == reflect.Ptr
			if ptr {
				vType = vType.Elem()
			}

			mapFields[i] = &singleValue{vType: vType, ptr: ptr}
		} else {
			mapFields[i] = &singleValue{vType: reflect.TypeOf("")}
		}
	}

	for i := lenCol; i < len(outTypes); i++ {
		mapFields[i] = &singleValue{vType: outTypes[i]}
	}

	return mapFields, nil
}

func (p *SQLParsed) makeMapField(col string, outType reflect.Type) selectItem {
	return &mapItem{k: reflect.ValueOf(col), vType: outType.Elem()}
}

func (p *SQLParsed) makeStructField(col string, outType reflect.Type) selectItem {
	fv, ok := outType.FieldByNameFunc(func(field string) bool {
		return matchesField2Col(outType, field, col)
	})

	if ok {
		return &structItem{StructField: &fv}
	}

	return nil
}

func (p *SQLParsed) makeVars(args []reflect.Value) []interface{} {
	vars := make([]interface{}, 0, len(p.Vars))

	for i, name := range p.Vars[:len(p.Vars)-len(p.fp.fieldVars)] {
		if p.BindBy == ByAuto {
			vars = append(vars, args[i].Interface())
		} else {
			seq, _ := strconv.Atoi(name)
			vars = append(vars, args[seq-1].Interface())
		}
	}

	if len(p.fp.fieldVars) > 0 {
		vars = append(vars, p.fp.fieldVars...)
	}

	return vars
}

func (p *SQLParsed) logError(err error) {
	log.Printf("E! error: %v", err)
	p.opt.Logger.LogError(err)
}

func convertExecResult(result sql.Result, stmt string, outTypes []reflect.Type) ([]reflect.Value, error) {
	if len(outTypes) == 0 {
		return []reflect.Value{}, nil
	}

	lastInsertIDVal, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("LastInsertId %s error %w", stmt, err)
	}

	rowsAffectedVal, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("RowsAffected %s error %w", stmt, err)
	}

	firstWord := strings.ToUpper(FirstWord(stmt))
	results := make([]reflect.Value, 0)

	if len(outTypes) == 1 {
		if firstWord == "INSERT" {
			return append(results, reflect.ValueOf(lastInsertIDVal).Convert(outTypes[0])), nil
		}

		return append(results, reflect.ValueOf(rowsAffectedVal).Convert(outTypes[0])), nil
	}

	results = append(results, reflect.ValueOf(rowsAffectedVal).Convert(outTypes[0]),
		reflect.ValueOf(lastInsertIDVal).Convert(outTypes[1]))

	for i := 2; i < len(outTypes); i++ {
		results = append(results, reflect.Zero(outTypes[i]))
	}

	return results, nil
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
	f.Set(val.Convert(f.Type()))
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

	out := make([]reflect.Value, len(outTypes))

	out0Kind := out0Type.Kind()
	hasParent := false
	switch out0Kind {
	case reflect.Map, reflect.Struct:
		hasParent = true
	}

	switch out0Kind {
	case reflect.Map:
		out0 = reflect.MakeMap(reflect.MapOf(out0Type.Key(), out0Type.Elem()))
		out[0] = out0
	default:
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
		} else if i == 0 {
			fv.ResetParent(out[0])
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
