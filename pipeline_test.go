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

package olric

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

func TestPipeline(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	buf := new(bytes.Buffer)
	dmap := "test-dmap"
	key := "test-key"
	rawval := "test-value"
	value, err := db.serializer.Marshal(rawval)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	req := protocol.NewDMapMessage(protocol.OpPut)
	req.SetBuffer(buf)
	req.SetDMap(dmap)
	req.SetKey(key)
	req.SetValue(value)
	req.SetExtra(protocol.PutExtra{Timestamp: time.Now().UnixNano()})
	err = req.Encode()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	preq := protocol.NewPipelineMessage(protocol.OpPipeline)
	preq.SetBuffer(new(bytes.Buffer))
	preq.SetValue(buf.Bytes())
	presp := preq.Response(nil)
	db.pipelineOperation(presp, preq)
	if presp.Status() != protocol.StatusOK {
		t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, presp.Status())
	}

	dm, err := db.NewDMap(dmap)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	val, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(string) != rawval {
		t.Fatalf("Expected value: %v. Got: %v", rawval, val)
	}
}
