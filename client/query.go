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

package client

import (
	"context"
	"sync"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/query"
	"github.com/hashicorp/go-multierror"
	"github.com/vmihailenco/msgpack"
	"golang.org/x/sync/semaphore"
)

// Cursor implements distributed query on DMaps. Call Cursor.Range to iterate over query results.
type Cursor struct {
	dm     *DMap
	query  []byte
	mu     sync.Mutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// Close cancels the underlying context and stops background goroutines.
func (c *Cursor) Close() {
	c.cancel()
}

func (c *Cursor) runQueryOnPartition(partID uint64) (olric.QueryResponse, error) {
	req := protocol.NewDMapMessage(protocol.OpQuery)
	req.SetDMap(c.dm.name)
	req.SetValue(c.query)
	req.SetExtra(protocol.QueryExtra{
		PartID: partID,
	})
	resp, err := c.dm.request(req)
	if err != nil {
		return nil, err
	}
	if err := checkStatusCode(resp); err != nil {
		return nil, err
	}

	var qr olric.QueryResponse
	err = msgpack.Unmarshal(resp.Value(), &qr)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func (c *Cursor) runQueryOnCluster(results chan olric.QueryResponse, errCh chan error) {
	defer c.wg.Done()
	defer close(results)

	var partID uint64
	var errs error
	var wg sync.WaitGroup

	appendError := func(e error) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		return multierror.Append(e, errs)
	}

	sem := semaphore.NewWeighted(olric.NumParallelQuery)
	for {
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
			defer wg.Done()
			defer sem.Release(1)

			resp, err := c.runQueryOnPartition(id)
			if err == olric.ErrEndOfQuery {
				c.Close()
				return
			}
			if err != nil {
				errs = appendError(err)
				c.Close()
				return
			}

			select {
			case <-c.ctx.Done():
				// cursor is gone:
				return
			default:
				results <- resp
			}
		}(partID)
		partID++
	}
	wg.Wait()
	errCh <- errs
}

// Range calls f sequentially for each key and value yielded from the cursor. If f returns false,
// range stops the iteration.
func (c *Cursor) Range(f func(key string, value interface{}) bool) error {
	defer c.Close()

	results := make(chan olric.QueryResponse, olric.NumParallelQuery)
	errCh := make(chan error, 1)

	c.wg.Add(1)
	go c.runQueryOnCluster(results, errCh)

	for result := range results {
		for key, rawval := range result {
			value, err := c.dm.unmarshalValue(rawval)
			if err != nil {
				return err
			}
			if !f(key, value) {
				// This means "break" on the client-side
				return nil
			}
		}
	}
	return <-errCh
}

// Query runs a distributed query on a dmap instance.
// Olric supports a very simple query DSL and now, it only scans keys. The query DSL has very
// few keywords:
//
// $onKey: Runs the given query on keys or manages options on keys for a given query.
//
// $onValue: Runs the given query on values or manages options on values for a given query.
//
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
func (d *DMap) Query(q query.M) (*Cursor, error) {
	if err := query.Validate(q); err != nil {
		return nil, err
	}
	qr, err := msgpack.Marshal(q)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Cursor{
		dm:     d,
		query:  qr,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}
