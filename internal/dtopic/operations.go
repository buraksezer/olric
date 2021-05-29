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
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/vmihailenco/msgpack"
)

func (s *Service) addListenerOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	name := req.DTopic()
	streamID := req.Extra().(protocol.DTopicAddListenerExtra).StreamID
	ss, err := s.streams.GetStreamByID(streamID)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	// Local listener
	listenerID := req.Extra().(protocol.DTopicAddListenerExtra).ListenerID

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-ss.Done():
		case <-s.ctx.Done():
		}
		err := s.dispatcher.removeListener(name, listenerID)
		if err != nil {
			s.log.V(4).Printf("[ERROR] ListenerID: %d could not be removed: %v", listenerID, err)
		}
		s.log.V(4).Printf("[INFO] ListenerID: %d has been removed", listenerID)
	}()

	f := func(msg Message) {
		ss, err := s.streams.GetStreamByID(streamID)
		if err != nil {
			s.log.V(4).Printf("[ERROR] Stream could not be found with the given StreamID: %d", streamID)
			err := s.dispatcher.removeListener(name, listenerID)
			if err != nil {
				s.log.V(4).Printf("[ERROR] Listener could not be removed with ListenerID: %d: %v", listenerID, err)
			}
			return
		}
		value, err := msgpack.Marshal(msg)
		if err != nil {
			s.log.V(4).Printf("[ERROR] Failed to serialize DTopicMessage: %v", err)
			return
		}
		m := protocol.NewDTopicMessage(protocol.OpStreamMessage)
		m.SetDTopic(name)
		m.SetValue(value)
		m.SetExtra(protocol.StreamMessageExtra{
			ListenerID: listenerID,
		})
		ss.Write(m)
	}
	// set concurrency parameter as 0. the registered listener will only make network i/o. NumCPU is good for this.
	err = s.dispatcher.addRemoteListener(listenerID, name, 0, f)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	// CurrentListeners is the current number of listeners of DTopics.
	CurrentListeners.Increase(1)

	// ListenersTotal is the total number of registered listeners during the life of this instance.
	ListenersTotal.Increase(1)

	w.SetStatus(protocol.StatusOK)
}

func (s *Service) removeListenerOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	extra, ok := req.Extra().(protocol.DTopicRemoveListenerExtra)
	if !ok {
		neterrors.ErrorResponse(w, neterrors.Wrap(neterrors.ErrInvalidArgument, "wrong extra type"))
		return
	}
	err := s.dispatcher.removeListener(req.DTopic(), extra.ListenerID)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	CurrentListeners.Decrease(1)
	w.SetStatus(protocol.StatusOK)
}
