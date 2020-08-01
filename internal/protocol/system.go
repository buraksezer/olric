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
	"encoding/binary"
	"fmt"
)

const SystemMessageHeaderSize uint32 = 3

const (
	MagicSystemReq MagicCode = 0xE8
	MagicSystemRes MagicCode = 0xE9
)

// Header defines a message header for both request and response.
type SystemMessageHeader struct {
	Op         OpCode     // 1
	ExtraLen   uint8      // 1
	StatusCode StatusCode // 1
}

type SystemMessage struct {
	Header
	SystemMessageHeader
	extra interface{}
	value []byte
	buf   *bytes.Buffer
}

func NewSystemMessage(opcode OpCode) *SystemMessage {
	return &SystemMessage{
		Header: Header{
			Magic:   MagicSystemReq,
			Version: Version1,
		},
		SystemMessageHeader: SystemMessageHeader{
			Op: opcode,
		},
	}
}

func NewSystemMessageFromRequest(buf *bytes.Buffer) *SystemMessage {
	return &SystemMessage{
		Header: Header{
			Magic:         MagicSystemReq,
			Version:       Version1,
			MessageLength: uint32(buf.Len()),
		},
		SystemMessageHeader: SystemMessageHeader{},
		buf:                 buf,
	}
}

func (d *SystemMessage) Response(buf *bytes.Buffer) EncodeDecoder {
	msg := &SystemMessage{
		Header: Header{
			Magic:   MagicSystemRes,
			Version: Version1,
		},
		SystemMessageHeader: SystemMessageHeader{
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

func (d *SystemMessage) SetStatus(code StatusCode) {
	d.StatusCode = code
}

func (d *SystemMessage) Status() StatusCode {
	return d.StatusCode
}

func (d *SystemMessage) SetValue(value []byte) {
	d.value = value
}

func (d *SystemMessage) Value() []byte {
	return d.value
}

func (d *SystemMessage) OpCode() OpCode {
	return d.Op
}

func (d *SystemMessage) SetBuffer(buf *bytes.Buffer) {
	d.buf = buf
}

func (d *SystemMessage) Buffer() *bytes.Buffer {
	return d.buf
}

func (d *SystemMessage) SetExtra(extra interface{}) {
	d.extra = extra
}

func (d *SystemMessage) Extra() interface{} {
	return d.extra
}

// Encode writes a protocol message to given TCP connection by encoding it.
func (d *SystemMessage) Encode() error {
	// Calculate lengths here
	if d.extra != nil {
		d.ExtraLen = uint8(binary.Size(d.extra))
	}
	d.MessageLength = SystemMessageHeaderSize + uint32(len(d.value)+int(d.ExtraLen))

	err := binary.Write(d.buf, binary.BigEndian, d.Header)
	if err != nil {
		return err
	}

	err = binary.Write(d.buf, binary.BigEndian, d.SystemMessageHeader)
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

func (d *SystemMessage) Decode() error {
	err := binary.Read(d.buf, binary.BigEndian, &d.SystemMessageHeader)
	if err != nil {
		return err
	}
	if d.Magic != MagicSystemReq && d.Magic != MagicSystemRes {
		return fmt.Errorf("invalid System message")
	}

	if d.Magic == MagicSystemReq && d.ExtraLen > 0 {
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
	vlen := int(d.MessageLength) - int(d.ExtraLen) - int(SystemMessageHeaderSize)
	if vlen != 0 {
		d.value = make([]byte, vlen)
		copy(d.value, d.buf.Next(vlen))
	}
	return nil
}
