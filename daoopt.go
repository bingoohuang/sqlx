package sqlx

import (
	"context"
	"fmt"
	"reflect"

	"github.com/bingoohuang/goreflect"

	"github.com/bingoohuang/goreflect/defaults"
)

type errorSetter func(error)

// CreateDaoOpt defines the options for CreateDao
type CreateDaoOpt struct {
	Error        *error
	Ctx          context.Context
	QueryMaxRows int `default:"-1"`

	RowScanInterceptor RowScanInterceptor

	DotSQL *DotSQL

	Logger DaoLogger
}

// CreateDaoOpter defines the option pattern interface for CreateDaoOpt.
type CreateDaoOpter interface {
	ApplyCreateOpt(*CreateDaoOpt)
}

// CreateDaoOptFn defines the func prototype to option applying.
type CreateDaoOptFn func(*CreateDaoOpt)

// ApplyCreateOpt applies the option.
func (c CreateDaoOptFn) ApplyCreateOpt(opt *CreateDaoOpt) { c(opt) }

// WithError specifies the err pointer to receive error.
func WithError(err *error) CreateDaoOpter {
	return CreateDaoOptFn(func(opt *CreateDaoOpt) { opt.Error = err })
}

// WithContext specifies the context.Context to db execution processes.
func WithContext(ctx context.Context) CreateDaoOpter {
	return CreateDaoOptFn(func(opt *CreateDaoOpt) { opt.Ctx = ctx })
}

// WithQueryMaxRows specifies the max rows to be fetched when execute query.
func WithQueryMaxRows(maxRows int) CreateDaoOpter {
	return CreateDaoOptFn(func(opt *CreateDaoOpt) { opt.QueryMaxRows = maxRows })
}

// WithLogger specifies dao logger.
func WithLogger(logger DaoLogger) CreateDaoOpter {
	return CreateDaoOptFn(func(opt *CreateDaoOpt) { opt.Logger = logger })
}

// WithSQLFile imports SQL queries from the file.
func WithSQLFile(sqlFile string) CreateDaoOpter {
	return CreateDaoOptFn(func(opt *CreateDaoOpt) {
		ds, err := DotSQLLoadFile(sqlFile)
		if err != nil {
			panic(err)
		}

		opt.DotSQL = ds
	})
}

// WithSQLStr imports SQL queries from the string.
func WithSQLStr(s string) CreateDaoOpter {
	return CreateDaoOptFn(func(opt *CreateDaoOpt) {
		ds, err := DotSQLLoadString(s)
		if err != nil {
			panic(err)
		}

		opt.DotSQL = ds
	})
}

// WithRowScanInterceptor specifies the RowScanInterceptor after a row fetched.
func WithRowScanInterceptor(interceptor RowScanInterceptor) CreateDaoOpter {
	return CreateDaoOptFn(func(opt *CreateDaoOpt) { opt.RowScanInterceptor = interceptor })
}

// RowScanInterceptor defines the interceptor after a row scanning.
type RowScanInterceptor interface {
	After(rowIndex int, v ...interface{}) (bool, error)
}

// RowScanInterceptorFn defines the interceptor function after a row scanning.
type RowScanInterceptorFn func(rowIndex int, v ...interface{}) (bool, error)

// After is revoked after after a row scanning.
func (r RowScanInterceptorFn) After(rowIndex int, v ...interface{}) (bool, error) {
	return r(rowIndex, v...)
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

		if f.PkgPath != "" /* not exportable? */ {
			continue
		}

		if !goreflect.IsError(f.Type) {
			continue
		}

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

	return func(err error) {
		if option.Error != nil {
			*option.Error = err
		}
	}
}
