# sqlmore
more about golang db sql

1. 数据库连接池增强

    * `MaxOpenConns:10`
    * `MaxIdleConns:0`
    * `ConnMaxLifetime:10s`

1. MySQL增强

    * 自动增强连接属性: `charset=utf8mb4&parseTime=true&loc=Local&timeout=10s&writeTimeout=10s&readTimeout=10s`
    * 增强GormDB建表选项: `db.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")`
    * MySQLDump
    
1. ExecSQL
1. SplitSqls

