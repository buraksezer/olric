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

/*package query implements data structures and helper functions for distributed query subsystem.*/
package query

import (
	"errors"
	"fmt"
	"github.com/vmihailenco/msgpack"
	"reflect"
)

// ErrInvalidQuery denotes a given query is empty or badly formatted.
var ErrInvalidQuery = errors.New("invalid query")

// M is an unordered representation of a query. This type should be used when the order of the elements does not matter.
// This type is handled as a regular map[string]interface{} when encoding and decoding. Elements will be serialized in
// an undefined, random order. (extracted from https://godoc.org/go.mongodb.org/mongo-driver/bson#M)
type M map[string]interface{}

// Validate validates a query.
func Validate(q M) error {
	if q == nil {
		return ErrInvalidQuery
	}
	for keyword, value := range q {
		switch keyword {
		case "$onKey", "$onValue", "$options":
			val, ok := value.(M)
			if !ok {
				return fmt.Errorf("wrong type for %s: %s, needs query.M",
					keyword, reflect.TypeOf(value))
			}
			err := Validate(val)
			if err != nil {
				return err
			}
		case "$regexMatch":
			_, ok := value.(string)
			if !ok {
				return fmt.Errorf("wrong type for %s: %s, needs string",
					keyword, reflect.TypeOf(value))
			}
		case "$ignore":
			_, ok := value.(bool)
			if !ok {
				return fmt.Errorf("wrong type for %s: %s, needs bool",
					keyword, reflect.TypeOf(value))
			}
		default:
			return fmt.Errorf("invalid keyword: %s", keyword)
		}
	}
	return nil
}

func buildQuery(q M) M {
	for keyword, value := range q {
		switch keyword {
		case "$onKey", "$onValue", "$options":
			q[keyword] = buildQuery(value.(map[string]interface{}))
		case "$regexMatch":
			q[keyword] = value.(string)
		case "$ignore":
			q[keyword] = value.(bool)
		}
	}
	return q
}

// FromByte generates a query from a byte slice.
func FromByte(data []byte) (M, error) {
	var q M
	if err := msgpack.Unmarshal(data, &q); err != nil {
		return nil, err
	}
	return buildQuery(q), nil
}
