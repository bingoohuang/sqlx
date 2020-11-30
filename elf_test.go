package sqlx_test

import (
	"testing"

	"github.com/bingoohuang/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestPkgName(t *testing.T) {
	assert.Equal(t, "abc", sqlx.FixPkgName("1ABC"))
	assert.Equal(t, "abc", sqlx.FixPkgName("a-bc"))
	assert.Equal(t, "abc", sqlx.FixPkgName("_abc"))
	assert.Equal(t, "abc1", sqlx.FixPkgName("a-bc1"))
}
