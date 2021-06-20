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

package dmap

import (
	"errors"
	"fmt"

	"github.com/buraksezer/olric/pkg/storage"
	"github.com/buraksezer/olric/query"
)

type queryPipeline struct {
	dm     *DMap
	partID uint64
	result queryResponse
}

func newQueryPipeline(dm *DMap, partID uint64) *queryPipeline {
	return &queryPipeline{
		dm:     dm,
		partID: partID,
		result: make(queryResponse),
	}
}

func (p *queryPipeline) doOnKey(q query.M) error {
	part := p.dm.s.primary.PartitionByID(p.partID)
	f, err := p.dm.loadFragment(part)
	if errors.Is(err, errFragmentNotFound) {
		// there is nothing to do
		return nil
	}
	if err != nil {
		return err
	}
	f.RLock()
	defer f.RUnlock()

	expr, ok := q["$regexMatch"].(string)
	if !ok {
		return fmt.Errorf("missing $regexMatch on $onKey")
	}
	nilValue, _ := p.dm.s.serializer.Marshal(nil)

	return f.storage.RegexMatchOnKeys(expr, func(hkey uint64, entry storage.Entry) bool {
		// Eliminate already expired k/v pairs
		if !isKeyExpired(entry.TTL()) {
			options, ok := q["$options"].(query.M)
			if ok {
				onValue, ok := options["$onValue"].(query.M)
				if ok {
					ignore, ok := onValue["$ignore"].(bool)
					if ok && ignore {
						entry.SetValue(nilValue)
					}
				}
			}
			p.result[hkey] = entry.Encode()
		}
		return true
	})
}

func (p *queryPipeline) execute(q query.M) (queryResponse, error) {
	if onKey, ok := q["$onKey"]; ok {
		if err := p.doOnKey(onKey.(query.M)); err != nil {
			return nil, err
		}
	}
	return p.result, nil
}
