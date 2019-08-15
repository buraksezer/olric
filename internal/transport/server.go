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

package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

// operations maps OpCodes to functions
type operations struct {
	m map[protocol.OpCode]protocol.Operation
}

const (
	idleConn uint32 = 0
	busyConn uint32 = 1
)

// Server implements a concurrent TCP server.
type Server struct {
	addr            string
	keepAlivePeriod time.Duration
	operations      operations
	logger          *log.Logger
	wg              sync.WaitGroup
	listener        net.Listener
	StartCh         chan struct{}
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewServer creates and returns a new Server.
func NewServer(addr string, logger *log.Logger, keepalivePeriod time.Duration) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	return &Server{
		operations:      operations{m: make(map[protocol.OpCode]protocol.Operation)},
		addr:            addr,
		keepAlivePeriod: keepalivePeriod,
		logger:          logger,
		StartCh:         make(chan struct{}),
		ctx:             ctx,
		cancel:          cancel,
	}
}

// RegisterOperation registers a function for the given OpCode.
func (s *Server) RegisterOperation(op protocol.OpCode, e protocol.Operation) {
	s.operations.m[op] = e
}

// GetOperation returns the function for the given OpCode.
func (s *Server) GetOperation(op protocol.OpCode) (protocol.Operation, error) {
	f, ok := s.operations.m[op]
	if !ok {
		return nil, fmt.Errorf("unknown operation")
	}
	return f, nil
}

// processRequest waits for a new request, handles it and returns the appropriate response.
func (s *Server) processRequest(req *protocol.Message, conn io.ReadWriter, connStatus *uint32) error {
	defer atomic.StoreUint32(connStatus, idleConn) // Mark connection as idle before start waiting a new request
	err := req.Read(conn)
	if err != nil {
		return errors.WithMessage(err, "failed to read request")
	}
	// Mark connection as busy.
	atomic.StoreUint32(connStatus, busyConn)
	opr, ok := s.operations.m[req.Op]
	if !ok {
		return fmt.Errorf("unknown operation: %d", req.Op)
	}
	resp := opr(req)
	err = resp.Write(conn)
	// WithMessage returns nil, if the err is nil.
	return errors.WithMessage(err, "failed to write response")
}

// processConn waits for requests and calls request handlers to generate a response. The connections are reusable.
func (s *Server) processConn(conn net.Conn) {
	defer s.wg.Done()

	// connStatus is useful for closing the server gracefully.
	var connStatus uint32
	done := make(chan struct{})
	defer close(done)

	// Control connection state and close it.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-s.ctx.Done():
			// The server is down.
		case <-done:
			// The following loop is quit. TCP socket may be closed or a protocol error occured.
		}

		if atomic.LoadUint32(&connStatus) != idleConn {
			s.logger.Printf("[DEBUG] Connection is busy, waiting")
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			// Wait for the current request. When it mark the connection as idle, break the loop.
			for {
				<-ticker.C
				if atomic.LoadUint32(&connStatus) == idleConn {
					s.logger.Printf("[DEBUG] Connection is idle, closing")
					break
				}
			}
		}

		// Close the connection and quit.
		if err := conn.Close(); err != nil {
			s.logger.Printf("[DEBUG] Failed to close TCP connection: %v", err)
		}
	}()

	for {
		var req protocol.Message
		// processRequest waits to read a message from the TCP socket.
		// Then calls its handler to generate a response.
		err := s.processRequest(&req, conn, &connStatus)
		if err != nil {
			// The socket probably would have been closed by the client.
			if errors.Cause(err) == io.EOF || errors.Cause(err) == protocol.ErrConnClosed {
				break
			}

			// Protocol error. Prepare an error message and return it.
			errResp := req.Error(protocol.StatusInternalServerError, err)
			err = errResp.Write(conn)
			if err != nil {
				// Failed to write to the socket. Fail early. This should be a bug or
				// the underlying TCP socket is unstable or unusable.
				s.logger.Printf("[ERROR] Failed to return error message: %v", err)
				break
			}
			// Continue waiting for incoming requests.
		}
	}
}

// listenAndServe calls Accept on given net.Listener.
func (s *Server) listenAndServe() error {
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
			s.logger.Printf("[DEBUG] Failed to accept TCP connection: %v", err)
			continue
		}
		if s.keepAlivePeriod.Seconds() != 0 {
			err = conn.(*net.TCPConn).SetKeepAlive(true)
			if err != nil {
				return err
			}
			err = conn.(*net.TCPConn).SetKeepAlivePeriod(s.keepAlivePeriod)
			if err != nil {
				return err
			}
		}
		s.wg.Add(1)
		go s.processConn(conn)
	}
}

// ListenAndServeTLS acts identically to ListenAndServe, except that it expects TLS connections.
func (s *Server) ListenAndServeTLS(cert, key string) error {
	defer func() {
		select {
		case <-s.StartCh:
			return
		default:
		}
		close(s.StartCh)
	}()

	c, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return err
	}
	config := &tls.Config{Certificates: []tls.Certificate{c}}
	l, err := tls.Listen("tcp", s.addr, config)
	if err != nil {
		return err
	}
	s.listener = l
	return s.listenAndServe()
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

	l, err := net.Listen("tcp", s.addr)
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
