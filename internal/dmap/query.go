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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/buraksezer/olric/query"
	"github.com/hashicorp/go-multierror"
	"github.com/vmihailenco/msgpack"
	"golang.org/x/sync/semaphore"
)

// NumConcurrentWorkers is the number of concurrent workers to run a query on the cluster.
const NumConcurrentWorkers = 2

// ErrEndOfQuery is the error returned by Range when no more data is available.
// Functions should return ErrEndOfQuery only to signal a graceful end of input.
var ErrEndOfQuery = neterrors.New(protocol.StatusErrEndOfQuery, "end of query")

// QueryResponse denotes returned data by a node for query.
type QueryResponse map[string]interface{}

// internal representation of query response
type queryResponse map[uint64][]byte

// Cursor implements distributed query on DMaps.
type Cursor struct {
	dm     *DMap
	query  query.M
	ctx    context.Context
	cancel context.CancelFunc
}

// Query runs a distributed query on a dmap instance.
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
	err := query.Validate(q)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Cursor{
		dm:     dm,
		query:  q,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (dm *DMap) runLocalQuery(partID uint64, q query.M) (queryResponse, error) {
	p := newQueryPipeline(dm, partID)
	return p.execute(q)
}

func (c *Cursor) reconcileResponses(responses []queryResponse) map[uint64]storage.Entry {
	result := make(map[uint64]storage.Entry)
	for _, response := range responses {
		for hkey, tmp1 := range response {
			val1 := c.dm.engine.NewEntry()
			val1.Decode(tmp1)

			if val2, ok := result[hkey]; ok {
				if val1.Timestamp() > val2.Timestamp() {
					result[hkey] = val1
				}
			} else {
				result[hkey] = val1
			}
		}
	}
	return result
}

func (c *Cursor) runQueryOnOwners(partID uint64) ([]storage.Entry, error) {
	value, err := msgpack.Marshal(c.query)
	if err != nil {
		return nil, err
	}

	owners := c.dm.s.primary.PartitionOwnersByID(partID)
	var responses []queryResponse
	for _, owner := range owners {
		if owner.CompareByID(c.dm.s.rt.This()) {
			response, err := c.dm.runLocalQuery(partID, c.query)
			if err != nil {
				return nil, err
			}
			responses = append(responses, response)
			continue
		}
		req := protocol.NewDMapMessage(protocol.OpLocalQuery)
		req.SetDMap(c.dm.name)
		req.SetValue(value)
		req.SetExtra(protocol.LocalQueryExtra{
			PartID: partID,
		})
		response, err := c.dm.s.requestTo(owner.String(), req)
		if err != nil {
			return nil, fmt.Errorf("query call is failed: %w", err)
		}

		tmp := make(queryResponse)
		err = msgpack.Unmarshal(response.Value(), &tmp)
		if err != nil {
			return nil, err
		}
		responses = append(responses, tmp)
	}

	var result []storage.Entry
	for _, entry := range c.reconcileResponses(responses) {
		result = append(result, entry)
	}
	return result, nil
}

func (c *Cursor) runQueryOnCluster(results chan []storage.Entry, errCh chan error) {
	defer c.dm.s.wg.Done()
	defer close(results)

	var mu sync.Mutex
	var wg sync.WaitGroup

	var errs error
	appendError := func(e error) error {
		mu.Lock()
		defer mu.Unlock()
		return multierror.Append(e, errs)
	}

	sem := semaphore.NewWeighted(NumConcurrentWorkers)
	for partID := uint64(0); partID < c.dm.s.config.PartitionCount; partID++ {
		err := sem.Acquire(c.ctx, 1)
		if errors.Is(err, context.Canceled) {
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
			case <-c.dm.s.ctx.Done():
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
	results := make(chan []storage.Entry, NumConcurrentWorkers)
	errCh := make(chan error, 1)

	c.dm.s.wg.Add(1)
	go c.runQueryOnCluster(results, errCh)

	for res := range results {
		for _, entry := range res {
			value, err := c.dm.unmarshalValue(entry.Value())
			if err != nil {
				return err
			}
			if !f(entry.Key(), value) {
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
