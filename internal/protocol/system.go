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

func (s *SystemMessage) Response(buf *bytes.Buffer) EncodeDecoder {
	msg := &SystemMessage{
		Header: Header{
			Magic:   MagicSystemRes,
			Version: Version1,
		},
		SystemMessageHeader: SystemMessageHeader{
			Op: s.Op,
		},
		buf: s.buf,
	}
	if buf != nil {
		msg.buf = buf
	} else {
		s.buf.Reset()
		msg.buf = s.buf
	}
	msg.MessageLength = uint32(s.buf.Len())
	return msg
}

func (s *SystemMessage) SetStatus(code StatusCode) {
	s.StatusCode = code
}

func (s *SystemMessage) Status() StatusCode {
	return s.StatusCode
}

func (s *SystemMessage) SetValue(value []byte) {
	s.value = value
}

func (s *SystemMessage) Value() []byte {
	return s.value
}

func (s *SystemMessage) OpCode() OpCode {
	return s.Op
}

func (s *SystemMessage) SetBuffer(buf *bytes.Buffer) {
	s.buf = buf
}

func (s *SystemMessage) Buffer() *bytes.Buffer {
	return s.buf
}

func (s *SystemMessage) SetExtra(extra interface{}) {
	s.extra = extra
}

func (s *SystemMessage) Extra() interface{} {
	return s.extra
}

// Encode writes a protocol message to given TCP connection by encoding it.
func (s *SystemMessage) Encode() error {
	// Calculate lengths here
	if s.extra != nil {
		s.ExtraLen = uint8(binary.Size(s.extra))
	}
	s.MessageLength = SystemMessageHeaderSize + uint32(len(s.value)+int(s.ExtraLen))

	err := binary.Write(s.buf, binary.BigEndian, s.Header)
	if err != nil {
		return err
	}

	err = binary.Write(s.buf, binary.BigEndian, s.SystemMessageHeader)
	if err != nil {
		return err
	}

	if s.extra != nil {
		err = binary.Write(s.buf, binary.BigEndian, s.extra)
		if err != nil {
			return err
		}
	}

	_, err = s.buf.Write(s.value)
	return err
}

func (s *SystemMessage) Decode() error {
	err := binary.Read(s.buf, binary.BigEndian, &s.SystemMessageHeader)
	if err != nil {
		return err
	}
	if s.Magic != MagicSystemReq && s.Magic != MagicSystemRes {
		return fmt.Errorf("invalid System message")
	}

	if s.Magic == MagicSystemReq && s.ExtraLen > 0 {
		raw := s.buf.Next(int(s.ExtraLen))
		extra, err := loadExtras(raw, s.Op)
		if err != nil {
			return err
		}
		s.extra = extra
	}

	// There is no maximum value for BodyLen which also includes ValueLen.
	// So our limit is available memory amount at the time of execution.
	// Please note that maximum partition size should not exceed 50MB for a smooth operation.
	vlen := int(s.MessageLength) - int(s.ExtraLen) - int(SystemMessageHeaderSize)
	if vlen != 0 {
		s.value = make([]byte, vlen)
		copy(s.value, s.buf.Next(vlen))
	}
	return nil
}
