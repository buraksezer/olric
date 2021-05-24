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

// ExpireExtra defines extra values for this operation.
type ExpireExtra struct {
	TTL       int64
	Timestamp int64
}

// UpdateRoutingExtra defines extra values for this operation.
type UpdateRoutingExtra struct {
	CoordinatorID uint64
}

type LocalQueryExtra struct {
	PartID uint64
}

type QueryExtra struct {
	PartID uint64
}

type StreamCreatedExtra struct {
	StreamID uint64
}

type StreamMessageExtra struct {
	ListenerID uint64
}

type DTopicAddListenerExtra struct {
	StreamID   uint64
	ListenerID uint64
}

type DTopicRemoveListenerExtra struct {
	ListenerID uint64
}

type StatsExtra struct {
	CollectRuntime bool
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
	case OpLocalQuery:
		extra := LocalQueryExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpQuery:
		extra := QueryExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpStreamCreated:
		extra := StreamCreatedExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpStreamMessage:
		extra := StreamMessageExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpDTopicAddListener:
		extra := DTopicAddListenerExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpDTopicRemoveListener:
		extra := DTopicRemoveListenerExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	case OpStats:
		extra := StatsExtra{}
		err := binary.Read(bytes.NewReader(raw), binary.BigEndian, &extra)
		return extra, err
	default:
		// Programming error
		return nil, fmt.Errorf("given OpCode: %v doesn't have extras", op)
	}
}
