# sqlx

[![Travis CI](https://img.shields.io/travis/bingoohuang/sqlx/master.svg?style=flat-square)](https://travis-ci.com/bingoohuang/sqlx)
[![Software License](https://img.shields.io/badge/License-MIT-orange.svg?style=flat-square)](https://github.com/bingoohuang/sqlx/blob/master/LICENSE.md)
[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/bingoohuang/sqlx)
[![Coverage Status](http://codecov.io/github/bingoohuang/sqlx/coverage.svg?branch=master)](http://codecov.io/github/bingoohuang/sqlx?branch=master)
[![goreport](https://www.goreportcard.com/badge/github.com/bingoohuang/sqlx)](https://www.goreportcard.com/report/github.com/bingoohuang/sqlx)

more about golang db sql

## 数据库连接池增强

1. `MaxOpenConns:10`
1. `MaxIdleConns:0`
1. `ConnMaxLifetime:10s`

## MySQL增强

1. 自动增强连接属性: `charset=utf8mb4&parseTime=true&loc=Local&timeout=10s&writeTimeout=10s&readTimeout=10s`
1. 增强GormDB建表选项: `db.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")`
1. MySQLDump

## Utilities

1. ExecSQL
1. SplitSqls

## Resources

1. [Interceptors for database/sql](https://github.com/ngrok/sqlmw)
1. [Generate type safe Go from SQL sqlc.dev](https://github.com/kyleconroy/sqlc)
1. [Golang SQL Database Layer for Layered Architecture. fs02.github.io/rel](https://github.com/Fs02/rel)
