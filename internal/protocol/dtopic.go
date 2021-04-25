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

// DTopicMessageHeaderSize defines total count of bytes in a DTopicMessage
const DTopicMessageHeaderSize uint32 = 5

const (
	// MagicDTopicReq is a magic number which denotes DTopic message requests on the wire.
	MagicDTopicReq MagicCode = 0xEA
	// MagicDTopicRes is a magic number which denotes DTopic message response on the wire.
	MagicDTopicRes MagicCode = 0xEB
)

// Header defines a message header for both request and response.
type DTopicMessageHeader struct {
	Op         OpCode     // 1
	DTopicLen  uint16     // 2
	ExtraLen   uint8      // 1
	StatusCode StatusCode // 1
}

// DTopicMessage is a message type in OBP. It can be used to access and modify DTopic data structure.
type DTopicMessage struct {
	Header
	DTopicMessageHeader
	dtopic string
	extra  interface{}
	value  []byte
	buf    *bytes.Buffer
}

// NewDTopicMessage returns a new DTopicMessage with the given operation code.
func NewDTopicMessage(opcode OpCode) *DTopicMessage {
	return &DTopicMessage{
		Header: Header{
			Magic:   MagicDTopicReq,
			Version: Version1,
		},
		DTopicMessageHeader: DTopicMessageHeader{
			Op: opcode,
		},
	}
}

// NewDTopicMessageFromRequest returns a new DTopicMessage for the given bytes.Buffer. The caller can use
// Decode method to read message from the raw data.
func NewDTopicMessageFromRequest(buf *bytes.Buffer) *DTopicMessage {
	return &DTopicMessage{
		Header: Header{
			Magic:         MagicDTopicReq,
			Version:       Version1,
			MessageLength: uint32(buf.Len()),
		},
		DTopicMessageHeader: DTopicMessageHeader{},
		buf:                 buf,
	}
}

// Response generates a response message for the request. This is a shortcut function to reduce boilerplate code.
func (d *DTopicMessage) Response(buf *bytes.Buffer) EncodeDecoder {
	msg := &DTopicMessage{
		Header: Header{
			Magic:   MagicDTopicRes,
			Version: Version1,
		},
		DTopicMessageHeader: DTopicMessageHeader{
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
func (d *DTopicMessage) SetStatus(code StatusCode) {
	d.StatusCode = code
}

// Status returns status code.
func (d *DTopicMessage) Status() StatusCode {
	return d.StatusCode
}

// SetValue writes the given byte slice into the underlying bytes.Buffer
func (d *DTopicMessage) SetValue(value []byte) {
	d.value = value
}

// Value returns the value
func (d *DTopicMessage) Value() []byte {
	return d.value
}

// OpCode returns operation code of the message
func (d *DTopicMessage) OpCode() OpCode {
	return d.Op
}

// SetBuffer sets the underlying bytes.Buffer. It should be recycled by the caller.
func (d *DTopicMessage) SetBuffer(buf *bytes.Buffer) {
	d.buf = buf
}

// Buffer returns the underlying bytes.Buffer
func (d *DTopicMessage) Buffer() *bytes.Buffer {
	return d.buf
}

// SetExtra sets the extra section for the message, if there is any.
func (d *DTopicMessage) SetExtra(extra interface{}) {
	d.extra = extra
}

// Extra returns the extra section of the message, if there is any.
func (d *DTopicMessage) Extra() interface{} {
	return d.extra
}

// SetDTopic sets the DTopic name for this message.
func (d *DTopicMessage) SetDTopic(dtopic string) {
	d.dtopic = dtopic
}

// DTopic returns the DTopic name.
func (d *DTopicMessage) DTopic() string {
	return d.dtopic
}

// Encode encodes the message into byte form.
func (d *DTopicMessage) Encode() error {
	// Calculate lengths here
	d.DTopicLen = uint16(len(d.dtopic))
	if d.extra != nil {
		d.ExtraLen = uint8(binary.Size(d.extra))
	}
	d.MessageLength = DTopicMessageHeaderSize + uint32(len(d.dtopic)+len(d.value)+int(d.ExtraLen))

	err := binary.Write(d.buf, binary.BigEndian, d.Header)
	if err != nil {
		return err
	}

	err = binary.Write(d.buf, binary.BigEndian, d.DTopicMessageHeader)
	if err != nil {
		return err
	}

	if d.extra != nil {
		err = binary.Write(d.buf, binary.BigEndian, d.extra)
		if err != nil {
			return err
		}
	}

	_, err = d.buf.WriteString(d.dtopic)
	if err != nil {
		return err
	}

	_, err = d.buf.Write(d.value)
	return err
}

// Decode decodes message from byte form into DTopicMessage.
func (d *DTopicMessage) Decode() error {
	err := binary.Read(d.buf, binary.BigEndian, &d.DTopicMessageHeader)
	if err != nil {
		return err
	}
	if d.Magic != MagicDTopicReq && d.Magic != MagicDTopicRes {
		return fmt.Errorf("invalid DTopic message")
	}

	if d.Magic == MagicDTopicReq && d.ExtraLen > 0 {
		raw := d.buf.Next(int(d.ExtraLen))
		extra, err := loadExtras(raw, d.Op)
		if err != nil {
			return err
		}
		d.extra = extra
	}

	d.dtopic = string(d.buf.Next(int(d.DTopicLen)))

	// There is no maximum value for BodyLen which also includes ValueLen.
	// So our limit is available memory amount at the time of execution.
	// Please note that maximum partition size should not exceed 50MB for a smooth operation.
	vlen := int(d.MessageLength) - int(d.ExtraLen) - int(d.DTopicLen) - int(DTopicMessageHeaderSize)
	if vlen != 0 {
		d.value = make([]byte, vlen)
		copy(d.value, d.buf.Next(vlen))
	}
	return nil
}
