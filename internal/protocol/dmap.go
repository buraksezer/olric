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

// DMapMessageHeaderSize defines total count of bytes in a DMapMessage
const DMapMessageHeaderSize uint32 = 7

const (
	// MagicDMapReq is a magic number which denotes DMap message requests on the wire.
	MagicDMapReq MagicCode = 0xE2
	// MagicDMapRes is a magic number which denotes DMap message response on the wire.
	MagicDMapRes MagicCode = 0xE3
)

// Header defines a message header for both request and response.
type DMapMessageHeader struct {
	Op         OpCode     // 1
	DMapLen    uint16     // 2
	KeyLen     uint16     // 2
	ExtraLen   uint8      // 1
	StatusCode StatusCode // 1
}

// DMapMessage is a message type in OBP. It can be used to access and modify DMap data structure.
type DMapMessage struct {
	Header
	DMapMessageHeader
	extra interface{}
	dmap  string
	key   string
	value []byte
	buf   *bytes.Buffer
}

// NewDMapMessage returns a new DMapMessage with the given operation code.
func NewDMapMessage(opcode OpCode) *DMapMessage {
	return &DMapMessage{
		Header: Header{
			Magic:   MagicDMapReq,
			Version: Version1,
		},
		DMapMessageHeader: DMapMessageHeader{
			Op: opcode,
		},
	}
}

// NewDMapMessageFromRequest returns a new DMapMessage for the given bytes.Buffer. The caller can use
// Decode method to read message from the raw data.
func NewDMapMessageFromRequest(buf *bytes.Buffer) *DMapMessage {
	return &DMapMessage{
		Header: Header{
			Magic:         MagicDMapReq,
			Version:       Version1,
			MessageLength: uint32(buf.Len()),
		},
		DMapMessageHeader: DMapMessageHeader{},
		buf:               buf,
	}
}

// Response generates a response message for the request. This is a shortcut function to reduce boilerplate code.
func (d *DMapMessage) Response(buf *bytes.Buffer) EncodeDecoder {
	msg := &DMapMessage{
		Header: Header{
			Magic:   MagicDMapRes,
			Version: Version1,
		},
		DMapMessageHeader: DMapMessageHeader{
			Op: d.Op,
		},
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
func (d *DMapMessage) SetStatus(code StatusCode) {
	d.StatusCode = code
}

// Status returns status code.
func (d *DMapMessage) Status() StatusCode {
	return d.StatusCode
}

// SetValue writes the given byte slice into the underlying bytes.Buffer
func (d *DMapMessage) SetValue(value []byte) {
	d.value = value
}

// Value returns the value
func (d *DMapMessage) Value() []byte {
	return d.value
}

// OpCode returns operation code of the message
func (d *DMapMessage) OpCode() OpCode {
	return d.Op
}

// SetBuffer sets the underlying bytes.Buffer. It should be recycled by the caller.
func (d *DMapMessage) SetBuffer(buf *bytes.Buffer) {
	d.buf = buf
}

// Buffer returns the underlying bytes.Buffer
func (d *DMapMessage) Buffer() *bytes.Buffer {
	return d.buf
}

// SetDMap sets the DMap name for this message.
func (d *DMapMessage) SetDMap(dmap string) {
	d.dmap = dmap
}

// Returns the DMap name.
func (d *DMapMessage) DMap() string {
	return d.dmap
}

// SetKey sets the key for this DMap message.
func (d *DMapMessage) SetKey(key string) {
	d.key = key
}

// Key returns the key for this DMap message.
func (d *DMapMessage) Key() string {
	return d.key
}

// SetExtra sets the extra section for the message, if there is any.
func (d *DMapMessage) SetExtra(extra interface{}) {
	d.extra = extra
}

// Extra returns the extra section of the message, if there is any.
func (d *DMapMessage) Extra() interface{} {
	return d.extra
}

// Encode encodes the message into byte form.
func (d *DMapMessage) Encode() error {
	// Calculate lengths here
	d.DMapLen = uint16(len(d.dmap))
	d.KeyLen = uint16(len(d.key))
	if d.extra != nil {
		d.ExtraLen = uint8(binary.Size(d.extra))
	}
	d.MessageLength = DMapMessageHeaderSize + uint32(len(d.dmap)+len(d.key)+len(d.value)+int(d.ExtraLen))

	err := binary.Write(d.buf, binary.BigEndian, d.Header)
	if err != nil {
		return err
	}

	err = binary.Write(d.buf, binary.BigEndian, d.DMapMessageHeader)
	if err != nil {
		return err
	}

	if d.extra != nil {
		err = binary.Write(d.buf, binary.BigEndian, d.extra)
		if err != nil {
			return err
		}
	}

	_, err = d.buf.WriteString(d.dmap)
	if err != nil {
		return err
	}

	_, err = d.buf.WriteString(d.key)
	if err != nil {
		return err
	}

	_, err = d.buf.Write(d.value)
	return err
}

// Decode decodes message from byte form into DMapMessage.
func (d *DMapMessage) Decode() error {
	err := binary.Read(d.buf, binary.BigEndian, &d.DMapMessageHeader)
	if err != nil {
		return err
	}
	if d.Magic != MagicDMapReq && d.Magic != MagicDMapRes {
		return fmt.Errorf("invalid dmap message")
	}

	if d.Magic == MagicDMapReq && d.ExtraLen > 0 {
		raw := d.buf.Next(int(d.ExtraLen))
		extra, err := loadExtras(raw, d.Op)
		if err != nil {
			return err
		}
		d.extra = extra
	}
	d.dmap = string(d.buf.Next(int(d.DMapLen)))
	d.key = string(d.buf.Next(int(d.KeyLen)))

	// There is no maximum value for BodyLen which also includes ValueLen.
	// So our limit is available memory amount at the time of execution.
	// Please note that maximum partition size should not exceed 50MB for a smooth operation.
	vlen := int(d.MessageLength) - int(d.ExtraLen) - int(d.KeyLen) - int(d.DMapLen) - int(DMapMessageHeaderSize)
	if vlen != 0 {
		d.value = make([]byte, vlen)
		copy(d.value, d.buf.Next(vlen))
	}
	return nil
}
