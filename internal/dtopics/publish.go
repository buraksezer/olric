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

package dtopics

import (
	"errors"
	"runtime"
	"time"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/vmihailenco/msgpack"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func (ds *DTopics) publishMessageOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	var msg Message
	err := msgpack.Unmarshal(req.Value(), &msg)
	if err != nil {
		w.SetStatus(protocol.StatusInternalServerError)
		w.SetValue([]byte(err.Error()))
		return
	}

	err = ds.dispatcher.dispatch(req.DTopic(), &msg)
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (ds *DTopics) publishDTopicMessageToAddr(member discovery.Member, topic string, msg *Message, sem *semaphore.Weighted) error {
	defer sem.Release(1)

	if member.CompareByID(ds.rt.This()) {
		// Dispatch messages in this process.
		err := ds.dispatcher.dispatch(topic, msg)
		if err != nil {
			if ds.log.V(6).Ok() {
				ds.log.V(6).Printf("[ERROR] Failed to dispatch message on this node: %v", err)
			}
			if !errors.Is(err, ErrInvalidArgument) {
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
	_, err = ds.client.RequestTo2(member.String(), req)
	if err != nil {
		ds.log.V(2).Printf("[ERROR] Failed to publish message to %s: %v", member, err)
		return err
	}
	return nil
}

func (ds *DTopics) publishDTopicMessage(topic string, msg *Message) error {
	ds.rt.Members().RLock()
	defer ds.rt.Members().RUnlock()

	// Propagate the message to the cluster in a parallel manner but
	// control concurrency. In order to prevent overloaded servers
	// because of network I/O, we use a semaphore.
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)
	var g errgroup.Group

	ds.rt.Members().Range(func(_ uint64, m discovery.Member) bool {
		member := m // https://golang.org/doc/faq#closures_and_goroutines
		ds.wg.Add(1)
		g.Go(func() error {
			defer ds.wg.Done()
			if !ds.isAlive() {
				return ErrServerGone
			}

			if err := sem.Acquire(ds.ctx, 1); err != nil {
				ds.log.V(3).Printf("[ERROR] Failed to acquire semaphore: %v", err)
				return err
			}
			return ds.publishDTopicMessageToAddr(member, topic, msg, sem)
		})
		return true
	})
	// Wait blocks until all function calls from the Go method have returned,
	// then returns the first non-nil error (if any) from them.
	return g.Wait()
}

func (ds *DTopics) exPublishOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	msg, err := ds.unmarshalValue(req.Value())
	if err != nil {
		errorResponse(w, err)
		return
	}
	tm := &Message{
		Message:       msg,
		PublisherAddr: "",
		PublishedAt:   time.Now().UnixNano(),
	}
	err = ds.publishDTopicMessage(req.DTopic(), tm)
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
