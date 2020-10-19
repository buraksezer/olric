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
	"fmt"

	"github.com/buraksezer/olric/internal/storage"
	"github.com/buraksezer/olric/query"
)

type queryPipeline struct {
	db     *Olric
	result queryResponse
}

func newQueryPipeline(db *Olric) *queryPipeline {
	return &queryPipeline{
		db:     db,
		result: make(queryResponse),
	}
}

func (p *queryPipeline) doOnKey(dm *dmap, q query.M) error {
	dm.RLock()
	defer dm.RUnlock()

	expr, ok := q["$regexMatch"].(string)
	if !ok {
		return fmt.Errorf("missing $regexMatch on $onKey")
	}
	nilValue, _ := p.db.serializer.Marshal(nil)

	return dm.storage.MatchOnKey(expr, func(hkey uint64, entry *storage.Entry) bool {
		// Eliminate already expired k/v pairs
		if !isKeyExpired(entry.TTL) {
			options, ok := q["$options"].(query.M)
			if ok {
				onValue, ok := options["$onValue"].(query.M)
				if ok {
					ignore, ok := onValue["$ignore"].(bool)
					if ok && ignore {
						entry.Value = nilValue
					}
				}
			}
			p.result[hkey] = entry
		}
		return true
	})
}

func (p *queryPipeline) execute(dm *dmap, q query.M) (queryResponse, error) {
	if onKey, ok := q["$onKey"]; ok {
		if err := p.doOnKey(dm, onKey.(query.M)); err != nil {
			return nil, err
		}
	}
	return p.result, nil
}
