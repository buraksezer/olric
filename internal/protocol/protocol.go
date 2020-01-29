// Copyright 2018-2019 Burak Sezer
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

/*Package protocol implements Olric Binary Protocol.*/
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

// pool is good for recycling memory while reading messages from the socket.
var pool *bufpool.BufPool = bufpool.New()

// MagicCode defines an unique code to distinguish a request message from a response message in Olric Binary Protocol.
type MagicCode uint8

const (
	// MagicReq defines an magic code for REQUEST in Olric Binary Protocol
	MagicReq MagicCode = 0xE2

	// MagicRes defines an magic code for RESPONSE in Olric Binary Protocol
	MagicRes MagicCode = 0xE3
)

type OpCode uint8

// ops
const (
	OpPut = OpCode(iota) + 1
	OpPutEx
	OpPutIf
	OpPutIfEx
	OpGet
	OpDelete
	OpDestroy
	OpLock
	OpLockWithTimeout
	OpUnlock
	OpIncr
	OpDecr
	OpGetPut
	OpUpdateRouting
	OpPutReplica
	OpPutIfReplica
	OpPutExReplica
	OpPutIfExReplica
	OpDeletePrev
	OpGetPrev
	OpGetBackup
	OpDeleteBackup
	OpDestroyDMap
	OpMoveDMap
	OpLengthOfPart
	OpPipeline
	OpPing
	OpStats
	OpExpire
	OpExpireReplica
)

type StatusCode uint8

// status codes
const (
	StatusOK = StatusCode(iota)
	StatusInternalServerError
	StatusBadRequest
	StatusErrKeyNotFound
	StatusErrNoSuchLock
	StatusErrLockNotAcquired
	StatusErrWriteQuorum
	StatusErrReadQuorum
	StatusErrOperationTimeout
	StatusErrKeyFound
	StatusErrClusterQuorum
	StatusErrUnknownOperation
)

const headerSize int64 = 12

// Header defines a message header for both request and response.
type Header struct {
	Magic    MagicCode  // 1
	Op       OpCode     // 1
	DMapLen  uint16     // 2
	KeyLen   uint16     // 2
	ExtraLen uint8      // 1
	Status   StatusCode // 1
	BodyLen  uint32     // 4
}

// Message defines a protocol message in Olric Binary Protocol.
type Message struct {
	Header             // [0..10]
	Extra  interface{} // [11..(m-1)] Command specific extras (In)
	DMap   string      // [m..(n-1)] DMap (as needed, length in Header)
	Key    string      // [n..(x-1)] Key (as needed, length in Header)
	Value  []byte      // [x..y] Value (as needed, length in Header)
}

// LockWithTimeoutExtra defines extra values for this operation.
type LockWithTimeoutExtra struct {
	Timeout  int64
	Deadline int64
}

// LockExtra defines extra values for this operation.
type LockExtra struct {
	Deadline int64
}

// PutExtra defines extra values for this operation.
type PutExtra struct {
	Timestamp int64
}

// PutExExtra defines extra values for this operation.
type PutExExtra struct {
	TTL       int64
	Timestamp int64
}

// PutIfExtra defines extra values for this operation.
type PutIfExtra struct {
	Flags     int16
	Timestamp int64
}

// PutIfExExtra defines extra values for this operation.
type PutIfExExtra struct {
	Flags     int16
	Timestamp int64
	TTL       int64
}

// LengthOfPartExtra defines extra values for this operation.
type LengthOfPartExtra struct {
	PartID uint64
	Backup bool
}

// AtomicExtra defines extra values for this operation.
type AtomicExtra struct {
	Timestamp int64
}

// ExpireExtrrea defines extra values for this operation.
type ExpireExtra struct {
	TTL       int64
	Timestamp int64
}

// UpdateRoutingExtra defines extra values for this operation.
type UpdateRoutingExtra struct {
	CoordinatorID uint64
}

// ErrConnClosed means that the underlying TCP connection has been closed
// by the client or operating system.
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

func loadExtras(raw []byte, op OpCode) (interface{}, error) {
	switch op {
	case OpPutEx, OpPutExReplica:
		extra := PutExExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpPut, OpPutReplica:
		extra := PutExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpLockWithTimeout:
		extra := LockWithTimeoutExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpLock:
		extra := LockExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpLengthOfPart:
		extra := LengthOfPartExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpIncr, OpDecr, OpGetPut:
		extra := AtomicExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpExpire, OpExpireReplica:
		extra := ExpireExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpPutIfEx, OpPutIfExReplica:
		extra := PutIfExExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpPutIf, OpPutIfReplica:
		extra := PutIfExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpUpdateRouting:
		extra := UpdateRoutingExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	default:
		// Programming error
		return nil, fmt.Errorf("given OpCode: %v doesn't have extras", op)
	}
}

// Read reads a whole protocol message(including the value) from given connection
// by decoding it.
func (m *Message) Read(conn io.Reader) error {
	buf := pool.Get()
	defer pool.Put(buf)

	_, err := io.CopyN(buf, conn, headerSize)
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

	// Read Key, DMap name and message extras here.
	_, err = io.CopyN(buf, conn, int64(m.BodyLen))
	if err != nil {
		return filterNetworkErrors(err)
	}

	if m.Magic == MagicReq && m.ExtraLen > 0 {
		raw := buf.Next(int(m.ExtraLen))
		extra, err := loadExtras(raw, m.Op)
		if err != nil {
			return err
		}
		m.Extra = extra
	}
	m.DMap = string(buf.Next(int(m.DMapLen)))
	m.Key = string(buf.Next(int(m.KeyLen)))

	// There is no maximum value for BodyLen which includes ValueLen.
	// So our limit is available memory amount at the time of operation.
	// Please note that maximum partition size should not exceed 50MB for a smooth operation.
	vlen := int(m.BodyLen) - int(m.ExtraLen) - int(m.KeyLen) - int(m.DMapLen)
	if vlen != 0 {
		m.Value = make([]byte, vlen)
		copy(m.Value, buf.Next(vlen))
	}
	return nil
}

// Write writes a protocol message to given TCP connection by encoding it.
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

// Error generates an error message for the request.
func (m *Message) Error(status StatusCode, err interface{}) *Message {
	getError := func(err interface{}) string {
		switch val := err.(type) {
		case string:
			return val
		case error:
			return val.Error()
		default:
			return ""
		}
	}

	return &Message{
		Header: Header{
			Magic:  MagicRes,
			Op:     m.Op,
			Status: status,
		},
		Value: []byte(getError(err)),
	}
}

// Success generates a success message for the request.
func (m *Message) Success() *Message {
	return &Message{
		Header: Header{
			Magic:  MagicRes,
			Op:     m.Op,
			Status: StatusOK,
		},
	}
}
