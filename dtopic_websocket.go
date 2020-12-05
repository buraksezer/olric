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

package olric

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
)

func (db *Olric) dtopicWebsocketSubscribe(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	c, err := db.http.WebsocketUpgrade(w, r, nil)
	if err != nil {
		db.httpErrorResponse(w, errors.Wrap(err, "failed to upgrade connection"))
		return
	}

	defer func() {
		if err := c.Close(); err != nil {
			db.log.V(3).Printf("[ERROR] Failed to close Websocket: %v", err)
		}
	}()

	for {
		mt, message, err := c.ReadMessage()
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to read a message from Websocket: %v", err)
			break
		}
		db.log.V(3).Printf("[INFO] Message from Websocket recv: %s", message)
		err = c.WriteMessage(mt, message)
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to write to a message Websocket: %v", err)
			break
		}
	}
}
