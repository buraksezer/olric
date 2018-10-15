package olricdb

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"reflect"

	"github.com/vmihailenco/msgpack"
)

// Serializer interface responsible for encoding/decoding values to transmit over network between OlricDB nodes.
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
	t := reflect.TypeOf(value)
	v := reflect.New(t).Elem().Interface()
	gob.Register(v)

	var res bytes.Buffer
	err := gob.NewEncoder(&res).Encode(&value)
	return res.Bytes(), err
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
