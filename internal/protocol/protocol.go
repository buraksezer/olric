// Copyright 2018 Burak Sezer
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
	"io"
	"strings"

	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/pkg/errors"
)

// MaxValueSize is 1MB by default.
var MaxValueSize = 1 << 20

var ErrValueTooBig = errors.New("value too big")

var pool *bufpool.BufPool = bufpool.New()

type Operation func(in *Message) (out *Message)

// Magic Codes
type MagicCode uint8

const (
	MagicReq MagicCode = 0xE2
	MagicRes MagicCode = 0xE3
)

type OpCode uint8

// ops
const (
	OpExPut OpCode = OpCode(iota)
	OpExPutEx
	OpExGet
	OpExDelete
	OpExDestroy
	OpExLockWithTimeout
	OpExUnlock
	OpExIncr
	OpExDecr
	OpExGetPut
	OpUpdateRouting
	OpPutBackup
	OpDeletePrev
	OpGetPrev
	OpGetBackup
	OpFindLock
	OpLockPrev
	OpUnlockPrev
	OpDeleteBackup
	OpDestroyDMap
	OpMoveDMap
	OpBackupMoveDMap
	OpIsPartEmpty
	OpIsBackupEmpty
)

type StatusCode uint8

// status codes
const (
	StatusOK = StatusCode(iota)
	StatusInternalServerError
	StatusKeyNotFound
	StatusNoSuchLock
	StatusPartNotEmpty
	StatusBackupNotEmpty
)

const HeaderSize int64 = 12

// total length    // 12
type Header struct {
	Magic    MagicCode  // 1
	Op       OpCode     // 1
	DMapLen  uint16     // 2
	KeyLen   uint16     // 2
	ExtraLen uint8      // 1
	Status   StatusCode // 1
	BodyLen  uint32     // 4
}

type Message struct {
	Header             // [0..10]
	Extra  interface{} // [11..(m-1)] Command specific extras (In)
	DMap   string      // [m..(n-1)] DMap (as needed, length in Header)
	Key    string      // [n..(x-1)] Key (as needed, length in Header)
	Value  []byte      // [x..y] Value (as needed, length in Header)
}

type LockWithTimeoutExtra struct {
	TTL int64
}

type PutExExtra struct {
	TTL int64
}

type IsPartEmptyExtra struct {
	PartID uint64
}

var ErrConnClosed = errors.New("connection closed")

func filterNetworkErrors(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		return ErrConnClosed
	}
	return err
}

func (m *Message) Read(conn io.Reader) error {
	buf := pool.Get()
	defer pool.Put(buf)

	_, err := io.CopyN(buf, conn, HeaderSize)
	if err != nil {
		return filterNetworkErrors(err)
	}
	err = binary.Read(buf, binary.BigEndian, &m.Header)
	if err != nil {
		return err
	}
	if m.Magic != MagicReq && m.Magic != MagicRes {
		return fmt.Errorf("invalid message")
	}

	vlen := int(m.BodyLen) - int(m.ExtraLen) - int(m.KeyLen) - int(m.DMapLen)
	if vlen > MaxValueSize {
		return ErrValueTooBig
	}

	_, err = io.CopyN(buf, conn, int64(m.BodyLen))
	if err != nil {
		return filterNetworkErrors(err)
	}
	// TODO: Move this block outside this function
	if m.Magic == MagicReq && m.ExtraLen > 0 {
		raw := buf.Next(int(m.ExtraLen))
		if m.Op == OpExPutEx {
			p := PutExExtra{}
			err = binary.Read(bytes.NewReader(raw), binary.BigEndian, &p)
			m.Extra = p
		} else if m.Op == OpExLockWithTimeout || m.Op == OpLockPrev {
			p := LockWithTimeoutExtra{}
			err = binary.Read(bytes.NewReader(raw), binary.BigEndian, &p)
			m.Extra = p
		} else if m.Op == OpIsPartEmpty || m.Op == OpIsBackupEmpty {
			p := IsPartEmptyExtra{}
			err = binary.Read(bytes.NewReader(raw), binary.BigEndian, &p)
			m.Extra = p
		}
		if err != nil {
			return err
		}
	}
	m.DMap = string(buf.Next(int(m.DMapLen)))
	m.Key = string(buf.Next(int(m.KeyLen)))
	if vlen != 0 {
		m.Value = make([]byte, vlen)
		copy(m.Value, buf.Next(vlen))
	}
	return nil
}

func (m *Message) Write(conn io.Writer) error {
	buf := pool.Get()
	defer pool.Put(buf)

	m.DMapLen = uint16(len(m.DMap))
	m.KeyLen = uint16(len(m.Key))
	if m.Extra != nil {
		m.ExtraLen = uint8(binary.Size(m.Extra))
	}
	m.BodyLen = uint32(len(m.DMap) + len(m.Key) + len(m.Value) + int(m.ExtraLen))
	err := binary.Write(buf, binary.BigEndian, m.Header)
	if err != nil {
		return err
	}

	if m.Extra != nil {
		err = binary.Write(buf, binary.BigEndian, m.Extra)
		if err != nil {
			return err
		}
	}

	_, err = buf.WriteString(m.DMap)
	if err != nil {
		return err
	}

	_, err = buf.WriteString(m.Key)
	if err != nil {
		return err
	}

	_, err = buf.Write(m.Value)
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(conn)
	return filterNetworkErrors(err)
}

func (m *Message) Error(status StatusCode, err interface{}) *Message {
	var value []byte
	switch err.(type) {
	case string:
		value = []byte(err.(string))
	case error:
		value = []byte(err.(error).Error())
	}
	return &Message{
		Header: Header{
			Magic:  MagicRes,
			Op:     m.Op,
			Status: status,
		},
		Value: value,
	}
}

func (m *Message) Success() *Message {
	return &Message{
		Header: Header{
			Magic:  MagicRes,
			Op:     m.Op,
			Status: StatusOK,
		},
	}
}
