package transaction

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransaction_Tx(t *testing.T) {
	t.Run("MutateCommand", func(t *testing.T) {
		m := NewMutateCommand("foobar")
		require.Equal(t, "foobar", m.Key())
		require.Equal(t, MutateCommandKind, m.Kind())
	})

	t.Run("ReadCommand", func(t *testing.T) {
		m := NewReadCommand("foobar")
		require.Equal(t, "foobar", m.Key())
		require.Equal(t, ReadCommandKind, m.Kind())
	})
}
