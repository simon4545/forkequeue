package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUniqRands(t *testing.T) {
	rands := UniqRands(10, 10)

	assert.Equal(t, 10, len(rands))
}
