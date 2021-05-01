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
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
	"golang.org/x/sync/errgroup"
)

func (s *Service) destroyDTopicInternalOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	s.dispatcher.destroy(req.DTopic())
	w.SetStatus(protocol.StatusOK)
}

func (s *Service) destroyDTopicOnCluster(topic string) error {
	s.rt.Members().RLock()
	defer s.rt.Members().RUnlock()

	var g errgroup.Group
	s.rt.Members().Range(func(_ uint64, m discovery.Member) bool {
		member := m // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			if !s.isAlive() {
				return ErrServerGone
			}
			req := protocol.NewDTopicMessage(protocol.OpDestroyDTopicInternal)
			req.SetDTopic(topic)
			_, err := s.requestTo(member.String(), req)
			if err != nil {
				s.log.V(2).Printf("[ERROR] Failed to call Destroy on %s, topic: %s : %v", member, topic, err)
				return err
			}
			return nil
		})
		return true
	})

	return g.Wait()
}

func (s *Service) dtopicDestroyOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	err := s.destroyDTopicOnCluster(req.DTopic())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
