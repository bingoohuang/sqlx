package sqlmore

import (
	"strings"

	"github.com/jinzhu/gorm"
)

// MySQLMore MySQL增强器
type MySQLMore struct {
	dbDriver string
}

// NewMySQLMore 新建MySQL增强器
func NewMySQLMore(dbDriver string) *MySQLMore {
	return &MySQLMore{dbDriver: dbDriver}
}

// Matches 是否匹配当前实现
func (m *MySQLMore) Matches() bool { return m.dbDriver == "mysql" }

// EnhanceURI 增强URI
func (m *MySQLMore) EnhanceURI(dbURI string) string {
	// user:pass@tcp(192.168.136.90:3307)/db?charset=utf8mb4&parseTime=true&loc=Local
	// refer
	// 1. https://github.com/go-sql-driver/mysql
	// 2. https://gorm.io/docs/connecting_to_the_database.html
	// 3. https://stackoverflow.com/questions/40527808/setting-tcp-timeout-for-sql-connection-in-go
	enhanced := attachParameter(dbURI, "charset", "utf8mb4")
	enhanced += attachParameter(dbURI, "parseTime", "true")
	enhanced += attachParameter(dbURI, "loc", "Local")
	enhanced += attachParameter(dbURI, "timeout", "10s")
	enhanced += attachParameter(dbURI, "writeTimeout", "10s")
	enhanced += attachParameter(dbURI, "readTimeout", "10s")

	if enhanced != "" && !strings.Contains(dbURI, "?") {
		enhanced = "?" + enhanced[1:]
	}

	return dbURI + enhanced
}

func attachParameter(dbURI, key, value string) string {
	if strings.Contains(dbURI, key+"=") {
		return ""
	}

	return "&" + key + "=" + value
}

// EnhanceGormDB 增强GormDB
func (m *MySQLMore) EnhanceGormDB(db *gorm.DB) *gorm.DB {
	return db.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
}
