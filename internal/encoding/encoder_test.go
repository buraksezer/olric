package encoding

import (
	"bytes"
	"encoding"
	"testing"

	"github.com/stretchr/testify/require"
)

type MyType struct{}

var _ encoding.BinaryMarshaler = (*MyType)(nil)

func (t *MyType) MarshalBinary() ([]byte, error) {
	return []byte("hello"), nil
}

func TestWriter_WriteArg(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	w := New(buf)
	value := uint64(345353)
	err := w.Encode(value)
	require.NoError(t, err)

	scannedValue := new(uint64)
	err = Scan(buf.Bytes(), scannedValue)
	require.NoError(t, err)
}
