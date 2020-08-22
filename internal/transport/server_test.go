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
	"context"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/flog"
	"github.com/buraksezer/olric/internal/protocol"
)

func dispatcher(w, _ protocol.EncodeDecoder) {
	w.SetStatus(protocol.StatusOK)
}

func getRandomAddr() (string, int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return "", 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", 0, err
	}
	defer l.Close()
	// Now, parse the obtained address
	host, rawPort, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}

func newServer() (*Server, error) {
	bindAddr, bindPort, err := getRandomAddr()
	if err != nil {
		return nil, err
	}
	l := log.New(os.Stdout, "transport-test: ", log.LstdFlags)
	fl := flog.New(l)
	fl.SetLevel(6)
	fl.ShowLineNumber(1)
	c := &ServerConfig{
		BindAddr:        bindAddr,
		BindPort:        bindPort,
		KeepAlivePeriod: time.Second,
		GracefulPeriod:  time.Second,
	}
	s := NewServer(c, fl)
	s.SetDispatcher(dispatcher)
	return s, nil
}

func TestServer_ListenAndServe(t *testing.T) {
	s, err := newServer()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
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
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("StartCh could not be closed")
	case <-s.StartCh:
		return
	}
}

func TestServer_ProcessConn(t *testing.T) {
	s, err := newServer()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
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

	<-s.StartCh
	cc := &ClientConfig{
		Addrs:   []string{s.listener.Addr().String()},
		MaxConn: 10,
	}
	c := NewClient(cc)

	t.Run("process DMapMessage", func(t *testing.T) {
		req := protocol.NewDMapMessage(protocol.OpPut)
		resp, err := c.Request(req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
		}
	})

	t.Run("process StreamMessage", func(t *testing.T) {
		req := protocol.NewStreamMessage(protocol.OpCreateStream)
		resp, err := c.Request(req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
		}
	})

	t.Run("process PipelineMessage", func(t *testing.T) {
		req := protocol.NewPipelineMessage(protocol.OpPipeline)
		resp, err := c.Request(req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
		}
	})

	t.Run("process SystemMessage", func(t *testing.T) {
		req := protocol.NewSystemMessage(protocol.OpUpdateRouting)
		resp, err := c.Request(req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
		}
	})

	t.Run("process DTopicMessage", func(t *testing.T) {
		req := protocol.NewDTopicMessage(protocol.OpDTopicPublish)
		resp, err := c.Request(req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if resp.Status() != protocol.StatusOK {
			t.Fatalf("Expected status: %v. Got: %v", protocol.StatusOK, resp.Status())
		}
	})
}

func TestServer_GracefulShutdown(t *testing.T) {
	s, err := newServer()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.SetDispatcher(func(w, _ protocol.EncodeDecoder) {
		// Quit after aborting. Dont leak any goroutine in tests!
		<-ctx.Done()
		w.SetStatus(protocol.StatusErrOperationTimeout) // Dummy
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

	<-s.StartCh

	// Create a client and make a request. It will never return.
	cc := &ClientConfig{
		Addrs: []string{s.listener.Addr().String()},
	}
	c := NewClient(cc)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := protocol.NewDMapMessage(protocol.OpPut)
		_, err = c.Request(req)
		if err != io.EOF {
			t.Fatalf("Expected io.EOF. Got: %v", err)
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
