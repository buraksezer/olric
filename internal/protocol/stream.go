// Copyright 2018-2020 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
)

// StreamMessageHeaderSize defines total count of bytes in a StreamMessage
const StreamMessageHeaderSize uint32 = 3

const (
	// MagicStreamReq is a magic number which denotes Stream message requests on the wire.
	MagicStreamReq MagicCode = 0xE4
	// MagicStreamRes is a magic number which denotes Stream message response on the wire.
	MagicStreamRes MagicCode = 0xE5
)

// Header defines a message header for both request and response.
type StreamMessageHeader struct {
	Op         OpCode     // 1
	ExtraLen   uint8      // 1
	StatusCode StatusCode // 1
}

// StreamMessage is a message type in OBP. It can be used to access and modify Stream data structure.
type StreamMessage struct {
	Header
	StreamMessageHeader
	extra  interface{}
	value  []byte
	buf    *bytes.Buffer
	conn   io.ReadWriteCloser
	cancel context.CancelFunc
}

// NewStreamMessage returns a new StreamMessage with the given operation code.
func NewStreamMessage(opcode OpCode) *StreamMessage {
	return &StreamMessage{
		Header: Header{
			Magic:   MagicStreamReq,
			Version: Version1,
		},
		StreamMessageHeader: StreamMessageHeader{
			Op: opcode,
		},
	}
}

func ConvertToStreamMessage(msg EncodeDecoder, listenerID uint64) *StreamMessage {
	str := NewStreamMessage(OpStreamMessage)
	str.SetValue(msg.Buffer().Bytes())
	str.SetExtra(StreamMessageExtra{
		ListenerID: listenerID,
	})
	return str
}

// NewStreamMessageFromRequest returns a new StreamMessage for the given bytes.Buffer. The caller can use
// Decode method to read message from the raw data.
func NewStreamMessageFromRequest(buf *bytes.Buffer) *StreamMessage {
	return &StreamMessage{
		Header: Header{
			Magic:         MagicStreamReq,
			Version:       Version1,
			MessageLength: uint32(buf.Len()),
		},
		StreamMessageHeader: StreamMessageHeader{},
		buf:                 buf,
	}
}

// Response generates a response message for the request. This is a shortcut function to reduce boilerplate code.
func (d *StreamMessage) Response(buf *bytes.Buffer) EncodeDecoder {
	msg := &StreamMessage{
		Header: Header{
			Magic:   MagicStreamRes,
			Version: Version1,
		},
		StreamMessageHeader: StreamMessageHeader{
			Op: d.Op,
		},
		buf: d.buf,
	}
	if buf != nil {
		msg.buf = buf
	} else {
		d.buf.Reset()
		msg.buf = d.buf
	}
	msg.MessageLength = uint32(d.buf.Len())
	return msg
}

// SetStatus sets a status code for the message.
func (d *StreamMessage) SetStatus(code StatusCode) {
	d.StatusCode = code
}

// Status returns status code.
func (d *StreamMessage) Status() StatusCode {
	return d.StatusCode
}

// SetValue writes the given byte slice into the underlying bytes.Buffer
func (d *StreamMessage) SetValue(value []byte) {
	d.value = value
}

// Value returns the value
func (d *StreamMessage) Value() []byte {
	return d.value
}

// OpCode returns operation code of the message
func (d *StreamMessage) OpCode() OpCode {
	return d.Op
}

// SetBuffer sets the underlying bytes.Buffer. It should be recycled by the caller.
func (d *StreamMessage) SetBuffer(buf *bytes.Buffer) {
	d.buf = buf
}

// Buffer returns the underlying bytes.Buffer
func (d *StreamMessage) Buffer() *bytes.Buffer {
	return d.buf
}

// SetExtra sets the extra section for the message, if there is any.
func (d *StreamMessage) SetExtra(extra interface{}) {
	d.extra = extra
}

// Extra returns the extra section of the message, if there is any.
func (d *StreamMessage) Extra() interface{} {
	return d.extra
}

func (d *StreamMessage) SetConn(conn io.ReadWriteCloser) {
	d.conn = conn
}

func (d *StreamMessage) Conn() io.ReadWriteCloser {
	return d.conn
}

func (d *StreamMessage) SetCancelFunc(f context.CancelFunc) {
	d.cancel = f
}

func (d *StreamMessage) Close() {
	if d.cancel == nil {
		return
	}
	d.cancel()
}

// Encode encodes the message into byte form.
func (d *StreamMessage) Encode() error {
	// Calculate lengths here
	if d.extra != nil {
		d.ExtraLen = uint8(binary.Size(d.extra))
	}
	d.MessageLength = StreamMessageHeaderSize + uint32(len(d.value)+int(d.ExtraLen))

	err := binary.Write(d.buf, binary.BigEndian, d.Header)
	if err != nil {
		return err
	}

	err = binary.Write(d.buf, binary.BigEndian, d.StreamMessageHeader)
	if err != nil {
		return err
	}

	if d.extra != nil {
		err = binary.Write(d.buf, binary.BigEndian, d.extra)
		if err != nil {
			return err
		}
	}

	_, err = d.buf.Write(d.value)
	return err
}

// Decode decodes message from byte form into StreamMessage.
func (d *StreamMessage) Decode() error {
	err := binary.Read(d.buf, binary.BigEndian, &d.StreamMessageHeader)
	if err != nil {
		return err
	}
	if d.Magic != MagicStreamReq && d.Magic != MagicStreamRes {
		return fmt.Errorf("invalid stream message")
	}

	if d.Magic == MagicStreamReq && d.ExtraLen > 0 {
		raw := d.buf.Next(int(d.ExtraLen))
		extra, err := loadExtras(raw, d.Op)
		if err != nil {
			return err
		}
		d.extra = extra
	}

	// There is no maximum value for BodyLen which also includes ValueLen.
	// So our limit is available memory amount at the time of execution.
	// Please note that maximum partition size should not exceed 50MB for a smooth operation.
	vlen := int(d.MessageLength) - int(d.ExtraLen) - int(StreamMessageHeaderSize)
	if vlen != 0 {
		d.value = make([]byte, vlen)
		copy(d.value, d.buf.Next(vlen))
	}
	return nil
}
