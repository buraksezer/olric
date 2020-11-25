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

package transport

import (
	"bytes"
	"context"
	"github.com/buraksezer/olric/config"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

func TestConnWithTimeout(t *testing.T) {
	var value = []byte("value")
	var cond int32

	s, err := newServer()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	s.SetDispatcher(func(w, _ protocol.EncodeDecoder) {
		if atomic.LoadInt32(&cond) == 0 {
			<-time.After(40 * time.Millisecond)
		} else {
			w.SetValue(value)
			w.SetStatus(protocol.StatusOK)
		}
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
		MaxConn:      10,
		ReadTimeout:  20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond,
	}
	cc.Sanitize()
	c := NewClient(cc)

	t.Run("Connection with i/o timeout", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			defer cancel()
			req := protocol.NewDMapMessage(protocol.OpPut)
			_, err := c.RequestTo(s.listener.Addr().String(), req)
			if err.(*net.OpError).Err.Error() != "i/o timeout" {
				t.Fatalf("Expected i/o timeout. Got: %v", err)
			}
		}()
		select {
		case <-time.After(250 * time.Millisecond):
			t.Fatal("The client connection is dead or something")
		case <-ctx.Done():
		}
	})

	t.Run("Connection without i/o timeout", func(t *testing.T) {
		atomic.StoreInt32(&cond, 1)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			defer cancel()
			req := protocol.NewDMapMessage(protocol.OpGet)
			resp, err := c.RequestTo(s.listener.Addr().String(), req)
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			if resp.Status() != protocol.StatusOK {
				t.Fatalf("Expected response status: %v. Got: %v", protocol.StatusOK, resp.Status())
			}
			if !bytes.Equal(resp.Value(), value) {
				t.Fatalf("Value in response is different")
			}
		}()

		select {
		case <-time.After(250 * time.Millisecond):
			t.Fatal("The client connection is dead or something")
		case <-ctx.Done():
		}
	})
}

func TestConnWithTimeout_Disabled(t *testing.T) {
	s, err := newServer()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	s.SetDispatcher(func(w, _ protocol.EncodeDecoder) {
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
		MaxConn:      10,
		ReadTimeout:  -1 * time.Millisecond,
		WriteTimeout: -1 * time.Millisecond,
	}
	cc.Sanitize()
	c := NewClient(cc)
	conn, err := c.conn(s.listener.Addr().String())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()
	_, ok := conn.(*ConnWithTimeout)
	if ok {
		t.Fatalf("conn is *ConnWithTimeout")
	}
}
