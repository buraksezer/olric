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
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/flog"
)

func dispatcher(w, _ protocol.EncodeDecoder) {
	w.SetStatus(protocol.StatusOK)
}

// getFreePort copied from testutil package to prevent cycle import.
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

func newServer(t *testing.T, f func(w, r protocol.EncodeDecoder)) *Server {
	bindPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	l := log.New(os.Stdout, "transport-test: ", log.LstdFlags)
	fl := flog.New(l)
	fl.SetLevel(6)
	fl.ShowLineNumber(1)
	c := &ServerConfig{
		BindAddr:        "127.0.0.1",
		BindPort:        bindPort,
		KeepAlivePeriod: time.Second,
		GracefulPeriod:  time.Second,
	}
	s := NewServer(c, fl)
	if f != nil {
		s.SetDispatcher(f)
	} else {
		s.SetDispatcher(dispatcher)
	}

	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()

	t.Cleanup(func() {
		err = s.Shutdown(context.Background())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	return s
}

func TestServer_ListenAndServe(t *testing.T) {
	s := newServer(t, nil)
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("StartedCtx could not be closed")
	case <-s.StartedCtx.Done():
		return
	}
}

func TestServer_ProcessConn(t *testing.T) {
	s := newServer(t, nil)

	<-s.StartedCtx.Done()
	cc := &config.Client{
		MaxConn: 10,
	}
	err := cc.Sanitize()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	c := NewClient(cc)

	t.Run("process DMapMessage", func(t *testing.T) {
		req := protocol.NewDMapMessage(protocol.OpPut)
		resp, err := c.RequestTo(s.listener.Addr().String(), req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
		}
	})

	t.Run("process StreamMessage", func(t *testing.T) {
		req := protocol.NewStreamMessage(protocol.OpCreateStream)
		resp, err := c.RequestTo(s.listener.Addr().String(), req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
		}
	})

	t.Run("process PipelineMessage", func(t *testing.T) {
		req := protocol.NewPipelineMessage(protocol.OpPipeline)
		resp, err := c.RequestTo(s.listener.Addr().String(), req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
		}
	})

	t.Run("process SystemMessage", func(t *testing.T) {
		req := protocol.NewSystemMessage(protocol.OpUpdateRouting)
		resp, err := c.RequestTo(s.listener.Addr().String(), req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
		}
	})

	t.Run("process DTopicMessage", func(t *testing.T) {
		req := protocol.NewDTopicMessage(protocol.OpDTopicPublish)
		resp, err := c.RequestTo(s.listener.Addr().String(), req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
		}
	})
}

func TestServer_GracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newServer(t, func(w, _ protocol.EncodeDecoder) {
		// Quit after aborting. Dont leak any goroutine in tests!
		<-ctx.Done()
		w.SetStatus(protocol.StatusErrOperationTimeout) // Dummy
	})

	<-s.StartedCtx.Done()

	// Create a client and make a request. It will never return.
	cc := &config.Client{}
	err := cc.Sanitize()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	c := NewClient(cc)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := protocol.NewDMapMessage(protocol.OpPut)
		_, err = c.RequestTo(s.listener.Addr().String(), req)
		if !errors.Is(err, io.EOF) {
			t.Errorf("Expected io.EOF. Got: %v", err)
		}
		cancel()
	}()
	// Await some time for goroutine scheduling
	<-time.After(100 * time.Millisecond)

	// Shutdown will kill the ongoing requests in 5 seconds.
	err = s.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	wg.Wait()
}

func TestServer_Statistics(t *testing.T) {
	s := newServer(t, nil)
	<-s.StartedCtx.Done()

	cc := &config.Client{
		MaxConn: 10,
	}
	err := cc.Sanitize()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	c := NewClient(cc)

	req := protocol.NewDMapMessage(protocol.OpPut)
	resp, err := c.RequestTo(s.listener.Addr().String(), req)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if resp.Status() != protocol.StatusOK {
		t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
	}

	var stats = map[string]int64{
		"CommandsTotal":      CommandsTotal.Read(),
		"ConnectionsTotal":   ConnectionsTotal.Read(),
		"CurrentConnections": CurrentConnections.Read(),
		"WrittenBytesTotal":  WrittenBytesTotal.Read(),
		"ReadBytesTotal":     ReadBytesTotal.Read(),
	}
	for name, value := range stats {
		if value == 0 {
			t.Fatalf("%s has to be bigger than zero", name)
		}
	}
}
