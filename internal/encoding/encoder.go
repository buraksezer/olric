package encoding

import (
	"encoding"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/buraksezer/olric/internal/util"
)

type encoder interface {
	io.Writer
	io.ByteWriter
	WriteString(s string) (n int, err error)
}

type Encoder struct {
	encoder

	lenBuf []byte
	numBuf []byte
}

func New(e encoder) *Encoder {
	return &Encoder{
		encoder: e,

		lenBuf: make([]byte, 64),
		numBuf: make([]byte, 64),
	}
}

func (e *Encoder) Encode(v interface{}) error {
	switch v := v.(type) {
	case nil:
		return e.string("")
	case string:
		return e.string(v)
	case []byte:
		return e.bytes(v)
	case int:
		return e.int(int64(v))
	case int8:
		return e.int(int64(v))
	case int16:
		return e.int(int64(v))
	case int32:
		return e.int(int64(v))
	case int64:
		return e.int(v)
	case uint:
		return e.uint(uint64(v))
	case uint8:
		return e.uint(uint64(v))
	case uint16:
		return e.uint(uint64(v))
	case uint32:
		return e.uint(uint64(v))
	case uint64:
		return e.uint(v)
	case float32:
		return e.float(float64(v))
	case float64:
		return e.float(v)
	case bool:
		if v {
			return e.int(1)
		}
		return e.int(0)
	case time.Time:
		e.numBuf = v.AppendFormat(e.numBuf[:0], time.RFC3339Nano)
		return e.bytes(e.numBuf)
	case time.Duration:
		return e.int(v.Nanoseconds())
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		return e.bytes(b)
	default:
		return fmt.Errorf(
			"olric: can't marshal %T (implement encoding.BinaryMarshaler)", v)
	}
}

func (e *Encoder) bytes(b []byte) error {
	if _, err := e.Write(b); err != nil {
		return err
	}
	return nil
}

func (e *Encoder) string(s string) error {
	return e.bytes(util.StringToBytes(s))
}

func (e *Encoder) uint(n uint64) error {
	e.numBuf = strconv.AppendUint(e.numBuf[:0], n, 10)
	return e.bytes(e.numBuf)
}

func (e *Encoder) int(n int64) error {
	e.numBuf = strconv.AppendInt(e.numBuf[:0], n, 10)
	return e.bytes(e.numBuf)
}

func (e *Encoder) float(f float64) error {
	e.numBuf = strconv.AppendFloat(e.numBuf[:0], f, 'f', -1, 64)
	return e.bytes(e.numBuf)
}
