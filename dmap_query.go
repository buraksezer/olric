// Copyright 2020 Burak Sezer
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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
	"github.com/buraksezer/olric/query"
	"github.com/hashicorp/go-multierror"
	"github.com/vmihailenco/msgpack"
	"golang.org/x/sync/semaphore"
)

const NumParallelQuery = 2

// ErrEndOfQuery is the error returned by Range when no more data is available.
// Functions should return ErrEndOfQuery only to signal a graceful end of input.
var ErrEndOfQuery = errors.New("end of query")

// QueryResponse denotes returned data by a node for query.
type QueryResponse map[string]interface{}

// internal representation of query response
type queryResponse map[uint64]*storage.VData

// Cursor implements distributed query on DMaps.
type Cursor struct {
	db     *Olric
	name   string
	query  query.M
	ctx    context.Context
	cancel context.CancelFunc
}

// Query runs a distributed query on a DMap instance.
// Olric supports a very simple query DSL and now, it only scans keys. The query DSL has very
// few keywords:
//
// $onKey: Runs the given query on keys or manages options on keys for a given query.
//
// $onValue: Runs the given query on values or manages options on values for a given query.
//
// $options: Useful to modify data returned from a query
//
// Keywords for $options:
//
// $ignore: Ignores a value.
//
// A distributed query looks like the following:
//
//   query.M{
// 	  "$onKey": query.M{
// 		  "$regexMatch": "^even:",
// 		  "$options": query.M{
// 			  "$onValue": query.M{
// 				  "$ignore": true,
// 			  },
// 		  },
// 	  },
//   }
//
// This query finds the keys starts with "even:", drops the values and returns only keys.
// If you also want to retrieve the values, just remove the $options directive:
//
//   query.M{
// 	  "$onKey": query.M{
// 		  "$regexMatch": "^even:",
// 	  },
//   }
//
// In order to iterate over all the keys:
//
//   query.M{
// 	  "$onKey": query.M{
// 		  "$regexMatch": "",
// 	  },
//   }
//
// Query function returns a cursor which has Range and Close methods. Please take look at the Range
// function for further info.
func (dm *DMap) Query(q query.M) (*Cursor, error) {
	ctx, cancel := context.WithCancel(context.Background())
	err := query.Validate(q)
	if err != nil {
		return nil, err
	}
	return &Cursor{
		db:     dm.db,
		name:   dm.name,
		query:  q,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (db *Olric) runLocalQuery(partID uint64, name string, q query.M) (queryResponse, error) {
	part := db.partitions[partID]
	dm, err := db.getOrCreateDMap(part, name)
	if err != nil {
		return nil, err
	}

	p := newQueryPipeline(db)
	result, err := p.execute(dm, q)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (db *Olric) localQueryOperation(req *protocol.Message) *protocol.Message {
	q, err := query.FromByte(req.Value)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}

	partID := req.Extra.(protocol.LocalQueryExtra).PartID
	result, err := db.runLocalQuery(partID, req.DMap, q)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}

	value, err := msgpack.Marshal(&result)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	resp := req.Success()
	resp.Value = value
	return resp
}

func (c *Cursor) reconcileResponses(responses []queryResponse) (queryResponse, error) {
	result := make(queryResponse)
	for _, response := range responses {
		for hkey, val1 := range response {
			if val2, ok := result[hkey]; ok {
				if val1.Timestamp > val2.Timestamp {
					result[hkey] = val1
				}
			} else {
				result[hkey] = val1
			}
		}
	}
	return result, nil
}

func (c *Cursor) runQueryOnOwners(partID uint64) ([]*storage.VData, error) {
	value, err := msgpack.Marshal(c.query)
	if err != nil {
		return nil, err
	}

	owners := c.db.partitions[partID].loadOwners()
	var responses []queryResponse
	for _, owner := range owners {
		if hostCmp(owner, c.db.this) {
			response, err := c.db.runLocalQuery(partID, c.name, c.query)
			if err != nil {
				return nil, err
			}
			responses = append(responses, response)

			continue
		}
		msg := &protocol.Message{
			DMap:  c.name,
			Value: value,
			Extra: protocol.LocalQueryExtra{
				PartID: partID,
			},
		}
		response, err := c.db.requestTo(owner.String(), protocol.OpLocalQuery, msg)
		if err != nil {
			return nil, fmt.Errorf("query call is failed: %w", err)
		}

		tmp := make(queryResponse)
		err = msgpack.Unmarshal(response.Value, &tmp)
		if err != nil {
			return nil, err
		}
		responses = append(responses, tmp)
	}
	res, err := c.reconcileResponses(responses)
	if err != nil {
		return nil, err
	}

	var result []*storage.VData
	for _, vdata := range res {
		result = append(result, vdata)
	}
	return result, nil
}

func (c *Cursor) runQueryOnCluster(results chan []*storage.VData, errCh chan error) {
	defer c.db.wg.Done()
	defer close(results)

	var mu sync.Mutex
	var wg sync.WaitGroup

	var errs error
	appendError := func(e error) error {
		mu.Lock()
		defer mu.Unlock()
		return multierror.Append(e, errs)
	}

	sem := semaphore.NewWeighted(NumParallelQuery)
	for partID := uint64(0); partID < c.db.config.PartitionCount; partID++ {
		err := sem.Acquire(c.ctx, 1)
		if err == context.Canceled {
			break
		}
		if err != nil {
			errs = appendError(err)
			break
		}

		wg.Add(1)
		go func(id uint64) {
			defer sem.Release(1)
			defer wg.Done()

			responses, err := c.runQueryOnOwners(id)
			if err != nil {
				errs = appendError(err)
				c.Close() // Breaks the loop
				return
			}
			select {
			case <-c.ctx.Done():
				// cursor is gone:
				return
			case <-c.db.ctx.Done():
				// Server is gone.
				return
			default:
				results <- responses
			}
		}(partID)
	}
	wg.Wait()
	errCh <- errs
}

// Range calls f sequentially for each key and value yielded from the cursor. If f returns false,
// range stops the iteration.
func (c *Cursor) Range(f func(key string, value interface{}) bool) error {
	defer c.Close()

	// Currently we have only 2 parallel query on the cluster. It's good enough for a smooth operation.
	results := make(chan []*storage.VData, NumParallelQuery)
	errCh := make(chan error, 1)

	c.db.wg.Add(1)
	go c.runQueryOnCluster(results, errCh)

	for res := range results {
		for _, vdata := range res {
			value, err := c.db.unmarshalValue(vdata.Value)
			if err != nil {
				return err
			}
			if !f(vdata.Key, value) {
				// User called "break" in this loop (Range)
				return nil
			}
		}
	}
	return <-errCh
}

// Close cancels the underlying context and background goroutines stops running.
func (c *Cursor) Close() {
	c.cancel()
}

func (db *Olric) exQueryOperation(req *protocol.Message) *protocol.Message {
	dm, err := db.NewDMap(req.DMap)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	q, err := query.FromByte(req.Value)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	c, err := dm.Query(q)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	defer c.Close()

	partID := req.Extra.(protocol.QueryExtra).PartID
	if partID >= db.config.PartitionCount {
		return req.Error(protocol.StatusErrEndOfQuery, ErrEndOfQuery)
	}
	responses, err := c.runQueryOnOwners(partID)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}

	data := make(QueryResponse)
	for _, response := range responses {
		data[response.Key] = response.Value
	}

	value, err := msgpack.Marshal(data)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	resp := req.Success()
	resp.Value = value
	return resp
}
