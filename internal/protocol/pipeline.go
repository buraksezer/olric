// Copyright 2018-2021 Burak Sezer
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
	"encoding/binary"
	"fmt"
)

// PipelineMessageHeaderSize defines total count of bytes in a PipelineMessage
const PipelineMessageHeaderSize uint32 = 3

const (
	// MagicPipelineReq is a magic number which denotes Pipeline message requests on the wire.
	MagicPipelineReq MagicCode = 0xE6
	// MagicPipelineRes is a magic number which denotes Pipeline message response on the wire.
	MagicPipelineRes MagicCode = 0xE7
)

// PipelineMessageHeader defines a message header for both request and response.
type PipelineMessageHeader struct {
	Op         OpCode     // 1
	ExtraLen   uint8      // 1
	StatusCode StatusCode // 1
}

// PipelineMessage is a message type in OBP. It can be used to access and modify Pipeline data structure.
type PipelineMessage struct {
	Header
	PipelineMessageHeader
	extra interface{}
	value []byte
	buf   *bytes.Buffer
}

// NewPipelineMessage returns a new PipelineMessage with the given operation code.
func NewPipelineMessage(opcode OpCode) *PipelineMessage {
	return &PipelineMessage{
		Header: Header{
			Magic:   MagicPipelineReq,
			Version: Version1,
		},
		PipelineMessageHeader: PipelineMessageHeader{
			Op: opcode,
		},
	}
}

// NewPipelineMessageFromRequest returns a new PipelineMessage for the given bytes.Buffer. The caller can use
// Decode method to read message from the raw data.
func NewPipelineMessageFromRequest(buf *bytes.Buffer) *PipelineMessage {
	return &PipelineMessage{
		Header: Header{
			Magic:         MagicPipelineReq,
			Version:       Version1,
			MessageLength: uint32(buf.Len()),
		},
		PipelineMessageHeader: PipelineMessageHeader{},
		buf:                   buf,
	}
}

// Response generates a response message for the request. This is a shortcut function to reduce boilerplate code.
func (d *PipelineMessage) Response(buf *bytes.Buffer) EncodeDecoder {
	msg := &PipelineMessage{
		Header: Header{
			Magic:   MagicPipelineRes,
			Version: Version1,
		},
		PipelineMessageHeader: PipelineMessageHeader{
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
func (d *PipelineMessage) SetStatus(code StatusCode) {
	d.StatusCode = code
}

// Status returns status code.
func (d *PipelineMessage) Status() StatusCode {
	return d.StatusCode
}

// SetValue writes the given byte slice into the underlying bytes.Buffer
func (d *PipelineMessage) SetValue(value []byte) {
	d.value = value
}

// Value returns the value
func (d *PipelineMessage) Value() []byte {
	return d.value
}

// OpCode returns operation code of the message
func (d *PipelineMessage) OpCode() OpCode {
	return d.Op
}

// SetBuffer sets the underlying bytes.Buffer. It should be recycled by the caller.
func (d *PipelineMessage) SetBuffer(buf *bytes.Buffer) {
	d.buf = buf
}

// Buffer returns the underlying bytes.Buffer
func (d *PipelineMessage) Buffer() *bytes.Buffer {
	return d.buf
}

// SetExtra sets the extra section for the message, if there is any.
func (d *PipelineMessage) SetExtra(extra interface{}) {
	d.extra = extra
}

// Extra returns the extra section of the message, if there is any.
func (d *PipelineMessage) Extra() interface{} {
	return d.extra
}

// Encode encodes the message into byte form.
func (d *PipelineMessage) Encode() error {
	// Calculate lengths here
	if d.extra != nil {
		d.ExtraLen = uint8(binary.Size(d.extra))
	}
	d.MessageLength = PipelineMessageHeaderSize + uint32(len(d.value)+int(d.ExtraLen))

	err := binary.Write(d.buf, binary.BigEndian, d.Header)
	if err != nil {
		return err
	}

	err = binary.Write(d.buf, binary.BigEndian, d.PipelineMessageHeader)
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

// Decode decodes message from byte form into PipelineMessage.
func (d *PipelineMessage) Decode() error {
	err := binary.Read(d.buf, binary.BigEndian, &d.PipelineMessageHeader)
	if err != nil {
		return err
	}
	if d.Magic != MagicPipelineReq && d.Magic != MagicPipelineRes {
		return fmt.Errorf("invalid Pipeline message")
	}

	// There is no maximum value for BodyLen which also includes ValueLen.
	// So our limit is available memory amount at the time of execution.
	// Please note that maximum partition size should not exceed 50MB for a smooth operation.
	vlen := int(d.MessageLength) - int(d.ExtraLen) - int(PipelineMessageHeaderSize)
	if vlen != 0 {
		d.value = make([]byte, vlen)
		copy(d.value, d.buf.Next(vlen))
	}
	return nil
}
