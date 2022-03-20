package resp

import (
	"bytes"
	"encoding"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type MyType struct{}

var _ encoding.BinaryMarshaler = (*MyType)(nil)

func (t *MyType) MarshalBinary() ([]byte, error) {
	return []byte("hello"), nil
}

func (t *MyType) UnmarshalBinary(data []byte) error {
	if !bytes.Equal([]byte("hello"), data) {
		return fmt.Errorf("not equal")
	}
	return nil
}

func TestWriter_WriteArg(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	w := New(buf)

	t.Run("uint64", func(t *testing.T) {
		defer buf.Reset()
		value := uint64(345353)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(uint64)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, uint64(345353), *scannedValue)
	})

	t.Run("nil", func(t *testing.T) {
		defer buf.Reset()

		err := w.Encode(nil)
		require.NoError(t, err)

		scannedValue := new(string)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, "", *scannedValue)
	})

	t.Run("string", func(t *testing.T) {
		defer buf.Reset()

		err := w.Encode("foobar")
		require.NoError(t, err)

		scannedValue := new(string)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, "foobar", *scannedValue)
	})

	t.Run("byte slice", func(t *testing.T) {
		defer buf.Reset()

		err := w.Encode([]byte("foobar"))
		require.NoError(t, err)

		scannedValue := new([]byte)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, []byte("foobar"), *scannedValue)
	})

	t.Run("int", func(t *testing.T) {
		defer buf.Reset()

		value := 345353
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(int)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, 345353, *scannedValue)
	})

	t.Run("int8", func(t *testing.T) {
		defer buf.Reset()

		value := int8(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(int8)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, int8(2), *scannedValue)
	})

	t.Run("int16", func(t *testing.T) {
		defer buf.Reset()

		value := int16(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(int16)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, int16(2), *scannedValue)
	})

	t.Run("int32", func(t *testing.T) {
		defer buf.Reset()

		value := int32(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(int32)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, int32(2), *scannedValue)
	})

	t.Run("int64", func(t *testing.T) {
		defer buf.Reset()

		value := int64(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(int64)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, int64(2), *scannedValue)
	})

	t.Run("uint", func(t *testing.T) {
		defer buf.Reset()

		value := uint(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(uint)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, uint(2), *scannedValue)
	})

	t.Run("uint8", func(t *testing.T) {
		defer buf.Reset()

		value := uint8(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(uint8)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, uint8(2), *scannedValue)
	})

	t.Run("uint16", func(t *testing.T) {
		defer buf.Reset()

		value := uint16(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(uint16)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, uint16(2), *scannedValue)
	})

	t.Run("uint32", func(t *testing.T) {
		defer buf.Reset()

		value := uint32(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(uint32)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, uint32(2), *scannedValue)
	})

	t.Run("uint64", func(t *testing.T) {
		defer buf.Reset()

		value := uint64(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(uint64)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, uint64(2), *scannedValue)
	})

	t.Run("float32", func(t *testing.T) {
		defer buf.Reset()

		value := float32(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(float32)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, float32(2), *scannedValue)
	})

	t.Run("float64", func(t *testing.T) {
		defer buf.Reset()

		value := float64(2)
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(float64)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, float64(2), *scannedValue)
	})

	t.Run("bool", func(t *testing.T) {
		defer buf.Reset()

		value := true
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(bool)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, true, *scannedValue)
	})

	t.Run("time.Time", func(t *testing.T) {
		defer buf.Reset()

		value := time.Now()
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(time.Time)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
	})

	t.Run("time.Duration", func(t *testing.T) {
		defer buf.Reset()

		value := time.Second
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(time.Duration)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, time.Second, *scannedValue)
	})

	t.Run("encoding.BinaryMarshaler", func(t *testing.T) {
		defer buf.Reset()

		var value encoding.BinaryMarshaler = &MyType{}
		err := w.Encode(value)
		require.NoError(t, err)

		scannedValue := new(MyType)
		err = Scan(buf.Bytes(), scannedValue)
		require.NoError(t, err)
		require.Equal(t, MyType{}, *scannedValue)
	})
}
