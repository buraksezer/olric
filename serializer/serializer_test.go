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

package serializer

import (
	"reflect"
	"testing"
)

type testStruct struct {
	StrVal string
	IntVal int
}

func TestGobSerializer(t *testing.T) {
	s := NewGobSerializer()
	tsOne := &testStruct{
		StrVal: "STRING",
		IntVal: 1988,
	}

	var err error
	var data []byte
	data, err = s.Marshal(tsOne)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if data == nil {
		t.Fatalf("Expected Gob encoded data. Got nil")
	}

	var tmp interface{}
	err = s.Unmarshal(data, &tmp)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	tsTwo := tmp.(*testStruct)
	if !reflect.DeepEqual(tsOne, tsTwo) {
		t.Fatalf("Unmarshaled data is different")
	}
}

func TestJsonSerializer(t *testing.T) {
	s := NewJSONSerializer()
	tsOne := &testStruct{
		StrVal: "STRING",
		IntVal: 1988,
	}

	var err error
	var data []byte
	data, err = s.Marshal(tsOne)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if data == nil {
		t.Fatalf("Expected Gob encoded data. Got nil")
	}

	var tsTwo testStruct
	err = s.Unmarshal(data, &tsTwo)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if !reflect.DeepEqual(*tsOne, tsTwo) {
		t.Fatalf("Unmarshaled data is different")
	}
}

func TestMsgPackSerializer(t *testing.T) {
	s := NewMsgpackSerializer()
	tsOne := &testStruct{
		StrVal: "STRING",
		IntVal: 1988,
	}

	var err error
	var data []byte
	data, err = s.Marshal(tsOne)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if data == nil {
		t.Fatalf("Expected Gob encoded data. Got nil")
	}

	var tsTwo testStruct
	err = s.Unmarshal(data, &tsTwo)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if !reflect.DeepEqual(*tsOne, tsTwo) {
		t.Fatalf("Unmarshaled data is different")
	}
}
