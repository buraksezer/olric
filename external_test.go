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

package olric

import (
	"context"
	"testing"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/transport"
)

func TestExternal_UnknownOperation(t *testing.T) {
	db, err := newOlric(nil)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	m := &protocol.Message{
		DMap:  "mydmap",
		Key:   "mykey",
		Value: []byte("myvalue"),
	}
	cc := &transport.ClientConfig{
		Addrs:   []string{db.config.Name},
		MaxConn: 10,
	}
	c := transport.NewClient(cc)
	resp, err := c.Request(protocol.OpCode(255), m)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if resp.Status != protocol.StatusInternalServerError {
		t.Fatalf("Expected status code: %d. Got: %d", protocol.StatusInternalServerError, resp.Status)
	}
}
