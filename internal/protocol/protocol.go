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

// Version1 denotes the first public version of Olric Binary Protocol.
var Version1 uint8 = 1

// pool is good for recycling memory while reading messages from the socket.
var pool = bufpool.New()

// MagicCode defines an unique code to distinguish a request message from a response message in Olric Binary Protocol.
type MagicCode uint8

// EncodeDecoder is an interface that defines methods for encoding/decoding a messages in OBP.
type EncodeDecoder interface {
	// Encode encodes the message and writes into a bytes.Buffer.
	Encode() error

	// Decode decodes the message from the given bytes.Buffer.
	Decode() error

	// SetStatus sets a status code for the message.
	SetStatus(StatusCode)

	// Status returns status code.
	Status() StatusCode

	// SetValue writes the given byte slice into the underlying bytes.Buffer
	SetValue([]byte)

	// Value returns the value
	Value() []byte

	// OpCode returns operation code of the message
	OpCode() OpCode

	// SetBuffer sets the underlying bytes.Buffer. It should be recycled by the caller.
	SetBuffer(*bytes.Buffer)

	// Buffer returns the underlying bytes.Buffer
	Buffer() *bytes.Buffer

	// SetExtra sets the extra section for the message, if there is any.
	SetExtra(interface{})

	// Extra returns the extra section of the message, if there is any.
	Extra() interface{}

	// Response generates a response message for the message.
	Response(*bytes.Buffer) EncodeDecoder
}

const HeaderLength int64 = 6

// Header is a shared message header for all the message types in Olric Binary Protocol.
type Header struct {
	Magic         MagicCode // 1 byte
	Version       uint8     // 1 byte
	MessageLength uint32    // 4 bytes
}

func checkProtocolVersion(header *Header) error {
	switch header.Version {
	case Version1:
		return nil
	default:
		return fmt.Errorf("unsupported protocol version: %d", header.Version)
	}
}

func readHeader(conn io.ReadWriteCloser) (*Header, error) {
	buf := pool.Get()
	defer pool.Put(buf)

	// Read the header section. The first 6 bytes.
	var header Header
	_, err := io.CopyN(buf, conn, HeaderLength)
	if err != nil {
		return nil, filterNetworkErrors(err)
	}

	err = binary.Read(buf, binary.BigEndian, &header)
	if err != nil {
		return nil, err
	}
	if err := checkProtocolVersion(&header); err != nil {
		return nil, err
	}
	return &header, nil
}

// ReadMessage reads the whole message from src into the given bytes.Buffer.
// Header can be used to determine message. Then you can pick an appropriate message
// type and decode it.
func ReadMessage(src io.ReadWriteCloser, dst *bytes.Buffer) (*Header, error) {
	header, err := readHeader(src)
	if err != nil {
		return nil, err
	}

	// Read the whole message. Now, the caller knows the message type and she can
	// Decode method.
	length := int64(header.MessageLength)
	nr, err := io.CopyN(dst, src, length)
	if err != nil {
		return nil, err
	}
	if nr != length {
		return nil, fmt.Errorf("byte count mismatch")
	}
	return header, nil
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

// BytesToConn translates bytes.Buffer into io.ReadWriteCloser interface. It's useful to implement
// pipeline in OBP.
type BytesToConn struct {
	*bytes.Buffer
}

// Close resets and recycles underlying bytes.Buffer.
func (b *BytesToConn) Close() error {
	pool.Put(b.Buffer)
	return nil
}

// NewBytesToConn returns a new BytesToConn. The underlying bytes.Buffer retrieves from the pool.
func NewBytesToConn(data []byte) *BytesToConn {
	b := pool.Get()
	b.Write(data)
	return &BytesToConn{
		Buffer: b,
	}
}
