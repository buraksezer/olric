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

package transport

import (
	"bytes"
	"context"
	"github.com/buraksezer/olric/config"
	"testing"

	"github.com/buraksezer/olric/internal/protocol"
)

func TestClient_Request(t *testing.T) {
	s, err := newServer()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	value := []byte("value")
	s.SetDispatcher(func(w, _ protocol.EncodeDecoder) {
		w.SetValue(value)
		w.SetStatus(protocol.StatusOK)
	})

	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()
	defer func() {
		err = s.Shutdown(context.TODO())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()
	<-s.StartedCtx.Done()

	cc := &config.Client{
		MaxConn: 10,
	}
	cc.Sanitize()
	c := NewClient(cc)

	t.Run("Request with round-robin", func(t *testing.T) {
		req := protocol.NewDMapMessage(protocol.OpPut)
		resp, err := c.RequestTo(s.listener.Addr().String(), req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %d. Got: %d", protocol.StatusOK, resp.Status())
		}
		if !bytes.Equal(resp.Value(), value) {
			t.Fatalf("Value in response is different")
		}
	})

	t.Run("Request without round-robin", func(t *testing.T) {
		req := protocol.NewDMapMessage(protocol.OpPut)
		resp, err := c.RequestTo(s.listener.Addr().String(), req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %d. Got: %d", protocol.StatusOK, resp.Status())
		}
		if !bytes.Equal(resp.Value(), value) {
			t.Fatalf("Value in response is different")
		}
	})

	t.Run("Close connection pool", func(t *testing.T) {
		c.ClosePool(s.listener.Addr().String())
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.pools) != 0 {
			t.Fatalf("Expected connection count in pool is zero. Got: %d", len(c.pools))
		}
	})
}
