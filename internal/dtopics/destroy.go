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
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"golang.org/x/sync/errgroup"
)

func (ds *DTopics) destroyOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	// req.DMap() is topic name in this context. This confusion will be fixed.
	ds.dispatcher.destroy(req.DTopic())
	w.SetStatus(protocol.StatusOK)
}

func (ds *DTopics) destroyDTopicOnCluster(topic string) error {
	ds.rt.Members().RLock()
	defer ds.rt.Members().RUnlock()

	var g errgroup.Group
	ds.rt.Members().Range(func(_ uint64, m discovery.Member) bool {
		member := m // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			if !ds.isAlive() {
				return ErrServerGone
			}
			req := protocol.NewDTopicMessage(protocol.OpDestroyDTopic)
			req.SetDTopic(topic)
			_, err := ds.client.RequestTo2(member.String(), req)
			if err != nil {
				ds.log.V(2).Printf("[ERROR] Failed to call Destroy on %s, topic: %s : %v", member, topic, err)
				return err
			}
			return nil
		})
		return true
	})

	return g.Wait()
}

func (ds *DTopics) exDestroyOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	err := ds.destroyDTopicOnCluster(req.DTopic())
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
