package shoveler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseByteSize(t *testing.T) {
	for in, want := range map[string]int{
		"":           0,
		"0":          0,
		"1073741824": 1 << 30,
		"512M":       512 << 20,
		"4G":         4 << 30,
		"4g":         4 << 30,
		"4GiB":       4 << 30,
		"16K":        16 << 10,
		"1T":         1 << 40,
	} {
		got, err := ParseByteSize(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, bad := range []string{"4X", "G", "-1G", "1.5G"} {
		_, err := ParseByteSize(bad)
		assert.Error(t, err, bad)
	}
}
