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
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/vmihailenco/msgpack"
)

func (ds *DTopics) exAddListenerOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	name := req.DTopic()
	streamID := req.Extra().(protocol.DTopicAddListenerExtra).StreamID
	ss, err := ds.streams.GetStreamById(streamID)
	if err != nil {
		errorResponse(w, err)
		return
	}

	// Local listener
	listenerID := req.Extra().(protocol.DTopicAddListenerExtra).ListenerID

	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		select {
		case <-ss.Done():
		case <-ds.ctx.Done():
		}
		err := ds.dispatcher.removeListener(name, listenerID)
		if err != nil {
			ds.log.V(4).Printf("[ERROR] ListenerID: %d could not be removed: %v", listenerID, err)
		}
		ds.log.V(4).Printf("[INFO] ListenerID: %d has been removed", listenerID)
	}()

	f := func(msg Message) {
		s, err := ds.streams.GetStreamById(streamID)
		if err != nil {
			ds.log.V(4).Printf("[ERROR] Stream could not be found with the given StreamID: %d", streamID)
			err := ds.dispatcher.removeListener(name, listenerID)
			if err != nil {
				ds.log.V(4).Printf("[ERROR] Listener could not be removed with ListenerID: %d: %v", listenerID, err)
			}
			return
		}
		value, err := msgpack.Marshal(msg)
		if err != nil {
			ds.log.V(4).Printf("[ERROR] Failed to serialize DTopicMessage: %v", err)
			return
		}
		m := protocol.NewDTopicMessage(protocol.OpStreamMessage)
		m.SetDTopic(name)
		m.SetValue(value)
		m.SetExtra(protocol.StreamMessageExtra{
			ListenerID: listenerID,
		})
		s.Write(m)
	}
	// set concurrency parameter as 0. the registered listener will only make network i/o. NumCPU is good for this.
	err = ds.dispatcher.addRemoteListener(listenerID, name, 0, f)
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
