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

package dtopic

import (
	"errors"
	"runtime"
	"time"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/vmihailenco/msgpack"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func (s *Service) publishMessageOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	var msg Message
	err := msgpack.Unmarshal(req.Value(), &msg)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	err = s.dispatcher.dispatch(req.DTopic(), &msg)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (s *Service) publishDTopicMessageToAddr(member discovery.Member, topic string, msg *Message, sem *semaphore.Weighted) error {
	defer sem.Release(1)

	if member.CompareByID(s.rt.This()) {
		// Dispatch messages in this process.
		err := s.dispatcher.dispatch(topic, msg)
		if err != nil {
			if s.log.V(6).Ok() {
				s.log.V(6).Printf("[ERROR] Failed to dispatch message on this node: %v", err)
			}
			if !errors.Is(err, neterrors.ErrInvalidArgument) {
				return err
			}
			return nil
		}
		return nil
	}
	data, err := msgpack.Marshal(*msg)
	if err != nil {
		return err
	}
	req := protocol.NewDTopicMessage(protocol.OpPublishDTopicMessage)
	req.SetDTopic(topic)
	req.SetValue(data)
	_, err = s.requestTo(member.String(), req)
	if err != nil {
		s.log.V(2).Printf("[ERROR] Failed to publish message to %s: %v", member, err)
		return err
	}
	return nil
}

func (s *Service) publishDTopicMessage(topic string, msg *Message) error {
	s.rt.Members().RLock()
	defer s.rt.Members().RUnlock()

	// Propagate the message to the cluster in a parallel manner but
	// control concurrency. In order to prevent overloaded servers
	// because of network I/O, we use a semaphore.
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)
	var g errgroup.Group

	s.rt.Members().Range(func(_ uint64, m discovery.Member) bool {
		member := m // https://golang.org/doc/faq#closures_and_goroutines
		s.wg.Add(1)
		g.Go(func() error {
			defer s.wg.Done()
			if !s.isAlive() {
				return ErrServerGone
			}

			if err := sem.Acquire(s.ctx, 1); err != nil {
				s.log.V(3).Printf("[ERROR] Failed to acquire semaphore: %v", err)
				return err
			}
			return s.publishDTopicMessageToAddr(member, topic, msg, sem)
		})
		return true
	})

	// PublishedTotal is the total number of published messages during the life of this instance.
	PublishedTotal.Increase(1)

	// Wait blocks until all function calls from the Go method have returned,
	// then returns the first non-nil error (if any) from them.
	return g.Wait()
}

func (s *Service) exPublishOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	msg, err := s.unmarshalValue(req.Value())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	tm := &Message{
		Message:       msg,
		PublisherAddr: "",
		PublishedAt:   time.Now().UnixNano(),
	}
	err = s.publishDTopicMessage(req.DTopic(), tm)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
