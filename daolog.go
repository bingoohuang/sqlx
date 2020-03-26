package sqlx

import (
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

var (
	_daoLoggerType = reflect.TypeOf((*DaoLogger)(nil)).Elem() // nolint gochecknoglobals
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

func createLogger(v reflect.Value, option *CreateDaoOpt) {
	if option.Logger != nil {
		return
	}

	for i := 0; i < v.NumField(); i++ {
		fv := v.Field(i)
		f := v.Type().Field(i)

		if f.PkgPath != "" /* not exportable? */ {
			continue
		}

		if goreflect.ImplType(f.Type, _daoLoggerType) && !fv.IsNil() {
			option.Logger = fv.Interface().(DaoLogger)
			return
		}
	}

	option.Logger = &DaoLoggerNoop{}
}
