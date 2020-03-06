package sqlx

import (
	"database/sql"
	"reflect"
)

// 参考 https://github.com/uber-go/dig/blob/master/types.go
// nolint gochecknoglobals
var (
	_errType        = reflect.TypeOf((*error)(nil)).Elem()
	_sqlScannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
)

// ImplType tells src whether it implements target type.
func ImplType(src, target reflect.Type) bool {
	if src.Kind() == reflect.Ptr {
		return src.Implements(target)
	}

	return reflect.PtrTo(src).Implements(target)
}

// ImplSQLScanner tells t whether it implements sql.Scanner interface.
func ImplSQLScanner(t reflect.Type) bool { return ImplType(t, _sqlScannerType) }

// IsError tells t whether it is error type exactly.
func IsError(t reflect.Type) bool { return t == _errType }
