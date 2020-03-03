package sqlmore

import (
	"database/sql"
	"time"

	"github.com/jinzhu/gorm"
)

// More 为各个数据库增强的接口
type More interface {
	// Matches 是否匹配当前实现
	Matches() bool
	// EnhanceURI 增强URI
	EnhanceURI(dbURI string) string
	// EnhanceGormDB 增强GormDB
	EnhanceGormDB(db *gorm.DB) *gorm.DB
}

// SQLMore SQL增强结构体
type SQLMore struct {
	more More

	// DbDriver 原始驱动名称
	DbDriver string
	// EnhancedDbURI 增强后的URI
	EnhancedDbURI string
}

// NewSQLMore 创建SQL增强器
func NewSQLMore(dbDriver, dbURI string) *SQLMore {
	mores := []More{NewMySQLMore(dbDriver)}
	sqlMore := &SQLMore{DbDriver: dbDriver}

	sqlMore.EnhancedDbURI = dbURI

	for _, m := range mores {
		if m.Matches() {
			sqlMore.more = m
			sqlMore.EnhancedDbURI = m.EnhanceURI(dbURI)

			break
		}
	}

	return sqlMore
}

// MustOpen 确保打开新的数据库连接池对象
func (s *SQLMore) MustOpen() *sql.DB {
	if db, err := s.Open(); err != nil {
		panic(err)
	} else {
		return db
	}
}

// Open 打开新的数据库连接池对象
func (s *SQLMore) Open() (*sql.DB, error) {
	db, err := sql.Open(s.DbDriver, s.EnhancedDbURI)
	if err != nil {
		return nil, err
	}

	return SetConnectionPool(db), nil
}

// MustGormOpen 确保打开新的Gorm数据库连接池对象
func (s *SQLMore) MustGormOpen() *gorm.DB {
	if db, err := s.GormOpen(); err != nil {
		panic(err)
	} else {
		return db
	}
}

// GormOpen 打开新的Gorm数据库连接池对象
func (s *SQLMore) GormOpen() (*gorm.DB, error) {
	db, err := gorm.Open(s.DbDriver, s.EnhancedDbURI)
	if err != nil {
		return nil, err
	}

	if s.more != nil {
		db = s.more.EnhanceGormDB(db)
	}

	SetConnectionPool(db.DB())

	return db, nil
}

// SetConnectionPool 设置连接池常见属性
func SetConnectionPool(db *sql.DB) *sql.DB {
	// 1. https://making.pusher.com/production-ready-connection-pooling-in-go/
	// 2. http://go-database-sql.org/connection-pool.html
	db.SetMaxOpenConns(10) // nolint gomnd
	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(10 * time.Second) // nolint gomnd

	return db
}
