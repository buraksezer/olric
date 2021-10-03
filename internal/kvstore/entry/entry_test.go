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

package entry

import (
	"reflect"
	"testing"
	"time"
)

func TestEntryEncodeDecode(t *testing.T) {
	entry := New()
	entry.SetKey("mykey")
	entry.SetTTL(200)
	entry.SetTimestamp(time.Now().UnixNano())
	entry.SetValue([]byte("mydata"))

	t.Run("Encode", func(t *testing.T) {
		buf := entry.Encode()
		if buf == nil {
			t.Fatal("Expected some data. Got nil")
		}

		t.Run("Decode", func(t *testing.T) {
			item := New()
			item.Decode(buf)
			if !reflect.DeepEqual(entry, item) {
				t.Fatal("Decoded Entry is different")
			}
		})
	})
}
