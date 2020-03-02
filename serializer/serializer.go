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

package serializer

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"reflect"

	"github.com/vmihailenco/msgpack"
)

// Serializer interface responsible for encoding/decoding values to transmit over network between Olric nodes.
type Serializer interface {
	// Marshal encodes v and returns a byte slice and possible error.
	Marshal(v interface{}) ([]byte, error)

	// Unmarshal decodes the encoded data and stores the result in the value pointed to by v.
	Unmarshal(data []byte, v interface{}) error
}

// Default serializer implementation which uses encoding/gob.
type gobSerializer struct{}

// NewGobSerializer returns a gob serializer. It's the default one.
func NewGobSerializer() Serializer {
	return Serializer(gobSerializer{})
}

func (g gobSerializer) Marshal(value interface{}) ([]byte, error) {
	if value != nil {
		t := reflect.TypeOf(value)
		v := reflect.New(t).Elem().Interface()
		gob.Register(v)
	}
	// TODO: As an optimization, you may want to a bytes.Buffer pool to reduce
	// memory consumption.
	var res bytes.Buffer
	err := gob.NewEncoder(&res).Encode(&value)
	if err != nil {
		return nil, err
	}
	return res.Bytes(), nil
}

func (g gobSerializer) Unmarshal(data []byte, v interface{}) error {
	r := bytes.NewBuffer(data)
	return gob.NewDecoder(r).Decode(v)
}

type jsonSerializer struct{}

func (j jsonSerializer) Marshal(v interface{}) ([]byte, error) { return json.Marshal(v) }

func (j jsonSerializer) Unmarshal(data []byte, v interface{}) error { return json.Unmarshal(data, v) }

// NewJSONSerializer returns a json serializer.
func NewJSONSerializer() Serializer {
	return Serializer(jsonSerializer{})
}

type msgpackSerializer struct{}

func (m msgpackSerializer) Marshal(v interface{}) ([]byte, error) { return msgpack.Marshal(v) }

func (m msgpackSerializer) Unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// NewMsgpackSerializer returns a msgpack serializer.
func NewMsgpackSerializer() Serializer {
	return Serializer(msgpackSerializer{})
}
