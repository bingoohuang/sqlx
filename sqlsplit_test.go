package sqlx_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/bingoohuang/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestPtr(t *testing.T) {
	n := time.Now()
	r := reflect.ValueOf(n)

	p := reflect.New(r.Type())
	p.Elem().Set(r)

	fmt.Println(p)
}

func TestSplitSql(t *testing.T) {
	assert := assert.New(t)

	sql := "create table aaa; drop table aaa;"
	sqls := sqlx.SplitSqls(sql, ';')

	assert.Equal([]string{"create table aaa", "drop table aaa"}, sqls)
}

func TestSplitSql2(t *testing.T) {
	assert := assert.New(t)

	sql := "ADD COLUMN `PREFERENTIAL_WAY` CHAR(3) NULL COMMENT '优\\惠方式:0:现金券;1:减免,2:赠送金额 ;' AFTER `PAY_TYPE`;"
	sqls := sqlx.SplitSqls(sql, ';')

	assert.Equal([]string{"ADD COLUMN `PREFERENTIAL_WAY` CHAR(3) NULL " +
		"COMMENT '优\\惠方式:0:现金券;1:减免,2:赠送金额 ;' AFTER `PAY_TYPE`"}, sqls)
}

func TestSplitSql3(t *testing.T) {
	assert := assert.New(t)

	sql := "ALTER TABLE `tt_l_mbrcard_chg`; \n" +
		"ADD COLUMN `PREFERENTIAL_WAY` CHAR(3) NULL COMMENT '优惠方式:''0:现金券;1:减免,2:赠送金额 ;' AFTER `PAY_TYPE`; "
	sqls := sqlx.SplitSqls(sql, ';')

	assert.Equal([]string{"ALTER TABLE `tt_l_mbrcard_chg`",
		"ADD COLUMN `PREFERENTIAL_WAY` CHAR(3) NULL " +
			"COMMENT '优惠方式:''0:现金券;1:减免,2:赠送金额 ;' AFTER `PAY_TYPE`"}, sqls)
}
