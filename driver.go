package sqlx

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"sync"
)

// nolint:gochecknoglobals
var (
	sqlDriverNamesByType     = map[reflect.Type]string{}
	sqlDriverNamesByTypeOnce sync.Once
)

// LookupDriverName get driverName from the driver instance.
// The database/sql API doesn't provide a way to get the registry name for
// a driver from the driver type.
// from https://github.com/golang/go/issues/12600
func LookupDriverName(driver driver.Driver) string {
	sqlDriverNamesByTypeOnce.Do(func() {
		for _, driverName := range sql.Drivers() {
			// Tested empty string DSN with MySQL, PostgreSQL, and SQLite3 drivers.
			if db, _ := sql.Open(driverName, ""); db != nil {
				sqlDriverNamesByType[reflect.TypeOf(db.Driver())] = driverName
			}
		}
	})

	return sqlDriverNamesByType[reflect.TypeOf(driver)]
}
