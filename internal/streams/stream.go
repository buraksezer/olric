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

package streams

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/flog"
	"golang.org/x/sync/errgroup"
)

// Stream provides a bidirectional communication channel between Olric nodes and clients. It can also be used
// for node-to-node communication.
type Stream struct {
	mu sync.RWMutex

	pingReceivedAt int64
	log            *flog.Logger
	conn           io.ReadWriteCloser
	read           chan protocol.EncodeDecoder
	write          chan protocol.EncodeDecoder
	ctx            context.Context
	cancel         context.CancelFunc
	errGr          errgroup.Group
}

// Close calls s.cancel and stops all background goroutines.
func (s *Stream) Close() {
	s.cancel()
}

func (s *Stream) Done() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Stream) Write(m protocol.EncodeDecoder) {
	s.write <- m
}

func (s *Stream) readFromStream(bufCh chan<- protocol.EncodeDecoder) error {
	defer s.cancel()

	f := func() error {
		buf := bufferPool.Get()
		defer bufferPool.Put(buf)

		header, err := protocol.ReadMessage(s.conn, buf)
		if err != nil {
			return err
		}

		var msg protocol.EncodeDecoder
		if header.Magic == protocol.MagicStreamReq {
			msg = protocol.NewStreamMessageFromRequest(buf)
			msg.(*protocol.StreamMessage).SetConn(s.conn)
		} else if header.Magic == protocol.MagicDMapReq {
			msg = protocol.NewDMapMessageFromRequest(buf)
		} else if header.Magic == protocol.MagicPipelineReq {
			msg = protocol.NewPipelineMessageFromRequest(buf)
		} else {
			return fmt.Errorf("invalid magic")
		}
		err = msg.Decode()
		if err != nil {
			return err
		}
		bufCh <- msg
		return nil
	}

	for {
		// this is good to manage bufferPool with defer statement
		if err := f(); err != nil {
			return err
		}
	}
}

func (s *Stream) readLoop() error {
	defer s.cancel()

	bufCh := make(chan protocol.EncodeDecoder, 1)
	s.errGr.Go(func() error {
		return s.readFromStream(bufCh)
	})

	for {
		select {
		case <-s.ctx.Done():
			return nil
		case buf := <-bufCh:
			if buf.OpCode() == protocol.OpStreamPing {
				s.setPingReceivedAt()
				s.write <- protocol.NewStreamMessage(protocol.OpStreamPong)
			} else {
				s.read <- buf
			}
		}
	}
}

func (s *Stream) writeToStream(msg protocol.EncodeDecoder) error {
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	msg.SetBuffer(buf)
	err := msg.Encode()
	if err != nil {
		return err
	}
	_, err = msg.Buffer().WriteTo(s.conn)
	return err
}

func (s *Stream) writeLoop() error {
	defer s.cancel()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		case msg := <-s.write:
			if err := s.writeToStream(msg); err != nil {
				return err
			}
		}
	}
}

func (s *Stream) setPingReceivedAt() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pingReceivedAt = time.Now().UnixNano()
}
