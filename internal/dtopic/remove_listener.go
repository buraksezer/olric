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

package dtopic

import (
	"fmt"
	"github.com/buraksezer/olric/internal/protocol"
)

func (s *Service) exRemoveListenerOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	extra, ok := req.Extra().(protocol.DTopicRemoveListenerExtra)
	if !ok {
		errorResponse(w, fmt.Errorf("%w: wrong extra type", ErrInvalidArgument))
		return
	}
	err := s.dispatcher.removeListener(req.DTopic(), extra.ListenerID)
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
