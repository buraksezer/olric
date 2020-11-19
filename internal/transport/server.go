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
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

const (
	idleConn uint32 = 0
	busyConn uint32 = 1
)

// pool is good for recycling memory while reading messages from the socket.
var bufferPool = bufpool.New()

// ErrInvalidMagic means that an OBP message is read from the TCP socket but
// the magic number is not valid.
var ErrInvalidMagic = errors.New("invalid magic")

// ServerConfig is a composite type to bundle configuration parameters.
type ServerConfig struct {
	BindAddr        string
	BindPort        int
	KeepAlivePeriod time.Duration
	// GracefulPeriod is useful to close busy connections when you want to shutdown the server.
	GracefulPeriod time.Duration
}

// Server implements a concurrent TCP server.
type Server struct {
	config     *ServerConfig
	log        *flog.Logger
	wg         sync.WaitGroup
	listener   net.Listener
	dispatcher func(w, r protocol.EncodeDecoder)
	StartCh    chan struct{}
	StopCh     chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewServer creates and returns a new Server.
func NewServer(c *ServerConfig, l *flog.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		config:  c,
		log:     l,
		StartCh: make(chan struct{}),
		StopCh:  make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func prepareRequest(header *protocol.Header, buf *bytes.Buffer) (protocol.EncodeDecoder, error) {
	var req protocol.EncodeDecoder
	if header.Magic == protocol.MagicDMapReq {
		req = protocol.NewDMapMessageFromRequest(buf)
	} else if header.Magic == protocol.MagicStreamReq {
		req = protocol.NewStreamMessageFromRequest(buf)
	} else if header.Magic == protocol.MagicPipelineReq {
		req = protocol.NewPipelineMessageFromRequest(buf)
	} else if header.Magic == protocol.MagicSystemReq {
		req = protocol.NewSystemMessageFromRequest(buf)
	} else if header.Magic == protocol.MagicDTopicReq {
		req = protocol.NewDTopicMessageFromRequest(buf)
	} else {
		return nil, errors.WithMessage(ErrInvalidMagic, fmt.Sprint(header.Magic))
	}
	return req, nil
}

func (s *Server) SetDispatcher(f func(w, r protocol.EncodeDecoder)) {
	s.dispatcher = f
}

func (s *Server) controlConnLifeCycle(conn io.ReadWriteCloser, connStatus *uint32, done chan struct{}) {
	// Control connection state and close it.
	defer s.wg.Done()

	select {
	case <-s.ctx.Done():
		// The server is down.
	case <-done:
		// The main loop is quit. TCP socket may be closed or a protocol error occurred.
	}

	if atomic.LoadUint32(connStatus) != idleConn {
		s.log.V(3).Printf("[DEBUG] Connection is busy, awaiting for %v", s.config.GracefulPeriod)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		//
		// WARNING: I added this context to fix a deadlock issue when an Olric node is being closed.
		// Debugging such an error is pretty hard and it blocks me. Normally I expect that SetDeadline
		// should fix the problem but It doesn't work. I don't know why. But this hack works well.
		//
		ctx, cancel := context.WithTimeout(context.Background(), s.config.GracefulPeriod)
		defer cancel()
	loop:
		for {
			select {
			// Wait for the current request. When it mark the connection as idle, break the loop.
			case <-ticker.C:
				if atomic.LoadUint32(connStatus) == idleConn {
					s.log.V(3).Printf("[DEBUG] Connection is idle, closing")
					break loop
				}
			case <-ctx.Done():
				s.log.V(3).Printf("[DEBUG] Connection is still in-use. Aborting.")
				break loop
			}
		}
	}

	// Close the connection and quit.
	if err := conn.Close(); err != nil {
		s.log.V(3).Printf("[DEBUG] Failed to close TCP connection: %v", err)
	}
}

func (s *Server) closeStream(req *protocol.StreamMessage, done chan struct{}) {
	defer s.wg.Done()
	defer req.Close()

	select {
	case <-done:
	case <-s.ctx.Done():
	}
}

// processMessage waits for a new request, handles it and returns the appropriate response.
func (s *Server) processMessage(conn io.ReadWriteCloser, connStatus *uint32, done chan struct{}) error {
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	header, err := protocol.ReadMessage(conn, buf)
	if err != nil {
		return err
	}

	req, err := prepareRequest(header, buf)
	if err != nil {
		return err
	}
	if header.Magic == protocol.MagicStreamReq {
		req.(*protocol.StreamMessage).SetConn(conn)
		s.wg.Add(1)
		go s.closeStream(req.(*protocol.StreamMessage), done)
	}

	// Decode reads the incoming message from the underlying TCP socket and parses
	err = req.Decode()
	if err != nil {
		return errors.WithMessage(err, "failed to read request")
	}

	// Mark connection as busy.
	atomic.StoreUint32(connStatus, busyConn)

	// Mark connection as idle before start waiting a new request
	defer atomic.StoreUint32(connStatus, idleConn)

	resp := req.Response(nil)
	// The dispatcher is defined by olric package and responsible to evaluate the incoming message.
	s.dispatcher(resp, req)
	err = resp.Encode()
	if err != nil {
		return err
	}
	_, err = resp.Buffer().WriteTo(conn)
	return err
}

// processConn waits for requests and calls request handlers to generate a response. The connections are reusable.
func (s *Server) processConn(conn io.ReadWriteCloser) {
	defer s.wg.Done()

	// connStatus is useful for closing the server gracefully.
	var connStatus uint32
	done := make(chan struct{})
	defer close(done)

	s.wg.Add(1)
	go s.controlConnLifeCycle(conn, &connStatus, done)

	for {
		// processMessage waits to read a message from the TCP socket.
		// Then calls its handler to generate a response.
		err := s.processMessage(conn, &connStatus, done)
		if err != nil {
			// The socket probably would have been closed by the client.
			if errors.Cause(err) == io.EOF || errors.Cause(err) == protocol.ErrConnClosed {
				s.log.V(5).Printf("[ERROR] End of the TCP connection: %v", err)
				break
			}
			s.log.V(5).Printf("[ERROR] Failed to process the incoming request: %v", err)
		}
	}
}

// listenAndServe calls Accept on given net.Listener.
func (s *Server) listenAndServe() error {
	defer close(s.StopCh)
	close(s.StartCh)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				// the server is closed. just quit.
				return nil
			default:
			}
			s.log.V(3).Printf("[DEBUG] Failed to accept TCP connection: %v", err)
			continue
		}
		if s.config.KeepAlivePeriod != 0 {
			err = conn.(*net.TCPConn).SetKeepAlive(true)
			if err != nil {
				return err
			}
			err = conn.(*net.TCPConn).SetKeepAlivePeriod(s.config.KeepAlivePeriod)
			if err != nil {
				return err
			}
		}
		s.wg.Add(1)
		go s.processConn(conn)
	}
}

// ListenAndServe listens on the TCP network address addr.
func (s *Server) ListenAndServe() error {
	defer func() {
		select {
		case <-s.StartCh:
			return
		default:
		}
		close(s.StartCh)
	}()

	if s.dispatcher == nil {
		return errors.New("no dispatcher found")
	}
	addr := net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort))
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = l
	return s.listenAndServe()
}

// Shutdown gracefully shuts down the server without interrupting any active connections.
// Shutdown works by first closing all open listeners, then closing all idle connections,
// and then waiting indefinitely for connections to return to idle and then shut down.
// If the provided context expires before the shutdown is complete, Shutdown returns
// the context's error, otherwise it returns any error returned from closing the Server's
// underlying Listener(s).
func (s *Server) Shutdown(ctx context.Context) error {
	select {
	case <-s.ctx.Done():
		// It's already closed.
		return nil
	default:
	}

	var result error
	s.cancel()
	err := s.listener.Close()
	if err != nil {
		result = multierror.Append(result, err)
	}

	// listener is gone.
	<-s.StopCh
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		err = ctx.Err()
		if err != nil {
			result = multierror.Append(result, err)
		}
	case <-done:
	}
	return result
}
