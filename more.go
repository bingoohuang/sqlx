package sqlx

import (
	"database/sql"
	"sync"
	"time"

	"github.com/jinzhu/gorm"
)

// More 为各个数据库增强的接口.
type More interface {
	// Matches 是否匹配当前实现
	Matches() bool
	// EnhanceURI 增强URI
	EnhanceURI(dbURI string) string
	// EnhanceGormDB 增强GormDB
	EnhanceGormDB(db *gorm.DB) *gorm.DB
}

// SQLMore SQL增强结构体.
type SQLMore struct {
	more More

	// DbDriver 原始驱动名称
	Driver string
	// EnhancedDbURI 增强后的URI
	EnhancedURI string
}

// NewSQLMore 创建SQL增强器.
func NewSQLMore(dbDriver, dbURI string) *SQLMore {
	mores := []More{NewMySQLMore(dbDriver)}
	sqlMore := &SQLMore{Driver: dbDriver}

	sqlMore.EnhancedURI = dbURI

	for _, m := range mores {
		if m.Matches() {
			sqlMore.more = m
			sqlMore.EnhancedURI = m.EnhanceURI(dbURI)

			break
		}
	}

	return sqlMore
}

// Open 确保打开新的数据库连接池对象.
func (s *SQLMore) Open() *sql.DB {
	if db, err := s.OpenE(); err != nil {
		panic(err)
	} else {
		return db
	}
}

// nolint:gochecknoglobals
var bindAddressOnce sync.Once

// OpenE 打开新的数据库连接池对象.
func (s *SQLMore) OpenE() (*sql.DB, error) {
	bindAddressOnce.Do(func() {
		if err := ViperMySQLBindAddress(); err != nil {
			panic(err)
		}
	})

	db, err := sql.Open(s.Driver, s.EnhancedURI)
	if err != nil {
		return nil, err
	}

	return SetConnectionPool(db), nil
}

// GormOpen 确保打开新的Gorm数据库连接池对象.
func (s *SQLMore) GormOpen() *gorm.DB {
	bindAddressOnce.Do(func() {
		if err := ViperMySQLBindAddress(); err != nil {
			panic(err)
		}
	})

	if db, err := s.GormOpenE(); err != nil {
		panic(err)
	} else {
		return db
	}
}

// GormOpenE 打开新的Gorm数据库连接池对象.
func (s *SQLMore) GormOpenE() (*gorm.DB, error) {
	db, err := gorm.Open(s.Driver, s.EnhancedURI)
	if err != nil {
		return nil, err
	}

	if s.more != nil {
		db = s.more.EnhanceGormDB(db)
	}

	SetConnectionPool(db.DB())

	return db, nil
}

// SetConnectionPool 设置连接池常见属性.
func SetConnectionPool(db *sql.DB) *sql.DB {
	// 1. https://making.pusher.com/production-ready-connection-pooling-in-go/
	// 2. http://go-database-sql.org/connection-pool.html
	db.SetMaxOpenConns(10) // nolint:gomnd
	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(10 * time.Second) // nolint:gomnd

	return db
}
