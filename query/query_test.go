// Copyright 2020 Burak Sezer
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

package query

import (
	"github.com/vmihailenco/msgpack"
	"reflect"
	"testing"
)

func TestQuery_FromByte(t *testing.T) {
	q := M{
		"$onKey": M{
			"$regexMatch": "even:",
			"$options": M{
				"$onValue": M{
					"$ignore": true,
				},
			},
		},
	}
	data, err := msgpack.Marshal(q)
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}
	qb, err := FromByte(data)
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}

	onKey, ok := qb["$onKey"]
	if !ok {
		t.Fatalf("$onKey could not be found")
	}

	onKey, ok = onKey.(M)
	if !ok {
		t.Fatalf("Invalid type for $onKey query")
	}
	options, ok := onKey.(M)["$options"]
	if !ok {
		t.Fatalf("Invalid type for $options query")
	}
	onValue, ok := options.(M)["$onValue"]
	if !ok {
		t.Fatalf("Invalid type for $onValue query")
	}
	ignore, ok := onValue.(M)["$ignore"]
	if !ok {
		t.Fatalf("Invalid type for $ignore query")
	}
	if reflect.TypeOf(ignore).Name() != "bool" {
		t.Fatalf("$ignore is not bool")
	}
}

func TestQuery_Validate(t *testing.T) {
	q1 := M{
		"$onKey": 10,
	}
	if err := Validate(q1); err == nil {
		t.Fatalf("Expected an error about data type. Got nil")
	}

	q2 := M{
		"$onKey": M{
			"$regexMatch": "even:",
			"$options": M{
				"$onValue": M{
					"$ignore": true,
				},
			},
		},
	}
	if err := Validate(q2); err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}
