package sqlx

import (
	"database/sql"
	"reflect"

	"github.com/sirupsen/logrus"

	"github.com/bingoohuang/goreflect"
)

// DaoLogger is the interface for dao logging.
type DaoLogger interface {
	// LogError logs the error
	LogError(err error)
	// LogStart logs the sql before the sql execution
	LogStart(id, sql string, vars interface{})
}

// nolint gochecknoglobals
var (
	_daoLoggerType = reflect.TypeOf((*DaoLogger)(nil)).Elem()
	_dbGetterType  = reflect.TypeOf((*DBGetter)(nil)).Elem()
)

// DaoLoggerNoop implements the interface for dao logging with NOOP.
type DaoLoggerNoop struct{}

// LogError logs the error
func (d *DaoLoggerNoop) LogError(err error) { /*NOOP*/ }

// LogStart logs the sql before the sql execution
func (d *DaoLoggerNoop) LogStart(id, sql string, vars interface{}) { /*NOOP*/ }

// DaoLogrus implements the interface for dao logging with logrus.
type DaoLogrus struct{}

// LogError logs the error
func (d *DaoLogrus) LogError(err error) {
	logrus.Warnf("error occurred %v", err)
}

// LogStart logs the sql before the sql execution
func (d *DaoLogrus) LogStart(id, sql string, vars interface{}) {
	logrus.Debugf("start to exec %s [%s] with %v", id, sql, vars)
}

func createDBGetter(v reflect.Value, option *CreateDaoOpt) {
	if option.DBGetter != nil {
		return
	}

	if fv := findTypedField(v, _dbGetterType); fv.IsValid() {
		option.DBGetter = fv.Interface().(DBGetter)
		return
	}

	option.DBGetter = GetDBFn(func() *sql.DB { return DB })
}

func createLogger(v reflect.Value, option *CreateDaoOpt) {
	if option.Logger != nil {
		return
	}

	if fv := findTypedField(v, _daoLoggerType); fv.IsValid() {
		option.Logger = fv.Interface().(DaoLogger)
		return
	}

	option.Logger = &DaoLoggerNoop{}
}

func findTypedField(v reflect.Value, t reflect.Type) reflect.Value {
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)

		if f.PkgPath != "" /* not exportable? */ {
			continue
		}

		fv := v.Field(i)
		if goreflect.ImplType(f.Type, t) && !fv.IsNil() {
			return fv
		}
	}

	return reflect.Value{}
}
