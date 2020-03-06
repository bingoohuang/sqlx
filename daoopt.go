package sqlx

import "context"

type errorSetter func(error)

// CreateDaoOpt defines the options for CreateDao
type CreateDaoOpt struct {
	Error        *error
	Ctx          context.Context
	QueryMaxRows int `default:"-1"`

	RowScanInterceptor RowScanInterceptor

	DotSQL *DotSQL
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
	After(rowIndex int, v interface{}) (bool, error)
}

// RowScanInterceptorFn defines the interceptor function after a row scanning.
type RowScanInterceptorFn func(rowIndex int, v interface{}) (bool, error)

// After is revoked after after a row scanning.
func (r RowScanInterceptorFn) After(rowIndex int, v interface{}) (bool, error) { return r(rowIndex, v) }
