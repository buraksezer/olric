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
	"reflect"
	"testing"
	"time"
)

type fakeTCPConn struct {
	*bytes.Buffer
}

func (f *fakeTCPConn) Close() error {
	return nil
}

func newFakeTCPConn(buf []byte) *fakeTCPConn {
	return &fakeTCPConn{
		Buffer: bytes.NewBuffer(buf),
	}
}

func TestDMapMessage_Encode(t *testing.T) {
	buf := new(bytes.Buffer)
	msg := NewDMapMessage(OpPut)
	msg.SetBuffer(buf)
	msg.SetDMap("mydmap")
	msg.SetKey("mykey")
	msg.SetValue([]byte("value"))
	err := msg.Encode()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDMapMessage_Decode(t *testing.T) {
	buf := new(bytes.Buffer)
	value := []byte("value")

	// Encode first
	msg := NewDMapMessage(OpPut)
	msg.SetBuffer(buf)
	msg.SetDMap("mydmap")
	msg.SetKey("mykey")
	msg.SetValue(value)
	msg.SetExtra(PutExtra{
		Timestamp: time.Now().UnixNano(),
	})

	err := msg.Encode()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	conn := newFakeTCPConn(buf.Bytes())
	buf.Reset()
	header, err := ReadMessage(conn, buf)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if header.Magic != MagicDMapReq {
		t.Fatalf("Expected MagicDMapReq (%d). Got: %d", MagicDMapReq, header.Magic)
	}

	// Decode message from the TCP socket
	req := NewDMapMessageFromRequest(buf)
	err = req.Decode()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if req.DMap() != "mydmap" {
		t.Fatalf("Expected mydmap. Got: %v", req.DMap())
	}

	if req.Key() != "mykey" {
		t.Fatalf("Expected mykey. Got: %v", req.Key())
	}

	if !bytes.Equal(req.Value(), value) {
		t.Fatalf("Expected myvalue. Got: %v", string(req.Value()))
	}

	if !reflect.DeepEqual(msg.Extra(), req.Extra()) {
		t.Fatalf("Different extra")
	}
}

func TestDMapMessage_Response(t *testing.T) {
	buf := new(bytes.Buffer)
	msg := NewDMapMessage(OpPut)
	msg.SetBuffer(buf)

	err := msg.Encode()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	respBuf := new(bytes.Buffer)
	resp := msg.Response(respBuf)
	if resp.OpCode() != msg.OpCode() {
		t.Fatalf("Expected OpCode: %d. Got: %d", msg.OpCode(), resp.OpCode())
	}

	value := []byte("value")
	resp.SetValue(value)
	if !bytes.Equal(resp.Value(), value) {
		t.Fatalf("response.Value() returned a different value")
	}

	resp.SetStatus(StatusInternalServerError)
	if resp.Status() != StatusInternalServerError {
		t.Fatalf("Expected status code: %d. Got: %d", StatusInternalServerError, resp.Status())
	}
}
