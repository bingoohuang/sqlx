package sqlx

import (
	"database/sql"
	"reflect"
	"time"
)

// NullAny represents any that may be null.
// NullAny implements the Scanner interface so it can be used as a scan destination.
type NullAny struct {
	Type reflect.Type
	Val  reflect.Value
}

// Scan assigns a value from a database driver.
//
// The src value will be of one of the following types:
//
//    int64
//    float64
//    bool
//    []byte
//    string
//    time.Time
//    nil - for NULL values
//
// An error should be returned if the value cannot be stored
// without loss of information.
//
// Reference types such as []byte are only valid until the next call to Scan
// and should not be retained. Their underlying memory is owned by the driver.
// If retention is necessary, copy their values before the next call to Scan.
func (n *NullAny) Scan(value interface{}) error {
	if n.Type == nil || value == nil {
		return nil
	}

	switch n.Type.Kind() {
	case reflect.String:
		sn := &sql.NullString{}
		if err := sn.Scan(value); err != nil {
			return err
		}

		n.Val = reflect.ValueOf(sn.String).Convert(n.Type)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		sn := &sql.NullInt32{}
		if err := sn.Scan(value); err != nil {
			return err
		}

		n.Val = reflect.ValueOf(sn.Int32).Convert(n.Type)
	case reflect.Float32, reflect.Float64:
		sn := &sql.NullFloat64{}
		if err := sn.Scan(value); err != nil {
			return err
		}

		n.Val = reflect.ValueOf(sn.Float64).Convert(n.Type)
	case reflect.Bool:
		sn := &sql.NullBool{}
		if err := sn.Scan(value); err != nil {
			return err
		}

		n.Val = reflect.ValueOf(sn.Bool).Convert(n.Type)
	case reflect.Interface:
		n.Val = reflect.ValueOf(value).Convert(n.Type)
	default:
		if n.Type == timeType || timeType.ConvertibleTo(n.Type) {
			sn := &sql.NullTime{}
			if err := sn.Scan(value); err != nil {
				return err
			}

			n.Val = reflect.ValueOf(sn.Time).Convert(n.Type)
		} else {
			sn := &sql.NullString{}
			if err := sn.Scan(value); err != nil {
				return err
			}

			n.Val = reflect.ValueOf(sn.String).Convert(n.Type)
		}
	}

	return nil
}

// nolint:gochecknoglobals
var (
	timeType = reflect.TypeOf((*time.Time)(nil)).Elem()
)

func (n *NullAny) getVal() reflect.Value {
	if n.Type == nil {
		return reflect.Value{}
	}

	if n.Val.IsValid() {
		return n.Val
	}

	return reflect.New(n.Type).Elem()
}
