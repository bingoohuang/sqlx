package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPkgName(t *testing.T) {
	assert.Equal(t, "abc", FixPkgName("1ABC"))
	assert.Equal(t, "abc", FixPkgName("a-bc"))
	assert.Equal(t, "abc", FixPkgName("_abc"))
	assert.Equal(t, "abc1", FixPkgName("a-bc1"))
}
