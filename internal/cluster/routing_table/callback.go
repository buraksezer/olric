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

package routing_table

func (r *RoutingTable) AddCallback(f func()) {
	r.callbackMtx.Lock()
	defer r.callbackMtx.Unlock()

	r.callbacks = append(r.callbacks, f)
}

func (r *RoutingTable) runCallbacks() {
	defer r.wg.Done()

	r.callbackMtx.Lock()
	defer r.callbackMtx.Unlock()

	for _, f := range r.callbacks {
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		f()
	}
}
