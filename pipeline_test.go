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

package olric

import (
	"bytes"
	"context"
	"github.com/buraksezer/olric/internal/protocol"
	"testing"
)

func TestPipeline(t *testing.T) {
	db, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	
	var buf bytes.Buffer
	dmap := "test-dmap"
	key := "test-key"
	rawval := "test-value"
	value, err := db.serializer.Marshal(rawval)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	req := protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op: protocol.OpPut,
		},
		DMap:   dmap,
		Key:    key,
		Value:  value,
	}
	err = req.Write(&buf)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	preq := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op: protocol.OpPipeline,
		},
		Value:  buf.Bytes(),
	}
	resp := db.pipelineOperation(preq)
	if resp.Status != protocol.StatusOK {
		t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status)
	}

	dm := db.NewDMap(dmap)
	val, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(string) == rawval {
		if err != nil {
			t.Fatalf("Expected value: %v. Got: %v", rawval, val)
		}
	}
}