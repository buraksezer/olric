// Copyright 2018-2022 Burak Sezer
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
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/resp"
	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var (
	// ErrNotReady denotes that the Future instance you hold is not ready to read the response yet.
	ErrNotReady = errors.New("not ready yet")

	// ErrPipelineClosed denotes that the underlying pipeline is closed, and it's impossible to operate.
	ErrPipelineClosed = errors.New("pipeline is closed")
)

// DMapPipeline implements a pipeline for the following methods of the DMap API:
//
// * Put
// * Get
// * Delete
// * Incr
// * Decr
// * GetPut
// * IncrByFloat
//
// DMapPipeline enables batch operations on DMap data.
type DMapPipeline struct {
	mtx      sync.Mutex
	dm       *ClusterDMap
	commands map[uint64][]redis.Cmder
	result   map[uint64][]redis.Cmder
	ctx      context.Context
	cancel   context.CancelFunc
}

func (dp *DMapPipeline) addCommand(key string, cmd redis.Cmder) (uint64, int) {
	dp.mtx.Lock()
	defer dp.mtx.Unlock()

	hkey := partitions.HKey(dp.dm.name, key)
	partID := hkey % dp.dm.clusterClient.partitionCount

	dp.commands[partID] = append(dp.commands[partID], cmd)

	return partID, len(dp.commands[partID]) - 1
}

// FuturePut is used to read the result of a pipelined Put command.
type FuturePut struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

// Result returns a response for the pipelined Put command.
func (f *FuturePut) Result() error {
	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		return processProtocolError(cmd.Err())
	default:
		return ErrNotReady
	}
}

// Put queues a Put command. The parameters are identical to the DMap.Put,
// but it returns FuturePut to read the batched response.
func (dp *DMapPipeline) Put(ctx context.Context, key string, value interface{}, options ...PutOption) (*FuturePut, error) {
	buf := bytes.NewBuffer(nil)

	enc := resp.New(buf)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}

	var pc dmap.PutConfig
	for _, opt := range options {
		opt(&pc)
	}

	cmd := dp.dm.writePutCommand(&pc, key, buf.Bytes()).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FuturePut{
		dp:     dp,
		partID: partID,
		index:  index,
		ctx:    dp.ctx,
	}, nil
}

// FutureGet is used to read result of a pipelined Get command.
type FutureGet struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

// Result returns a response for the pipelined Get command.
func (f *FutureGet) Result() (*GetResponse, error) {
	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() != nil {
			return nil, processProtocolError(cmd.Err())
		}
		stringCmd := redis.NewStringCmd(context.Background(), cmd.Args()...)
		stringCmd.SetVal(cmd.(*redis.Cmd).Val().(string))
		return f.dp.dm.makeGetResponse(stringCmd)
	default:
		return nil, ErrNotReady
	}
}

// Get queues a Get command. The parameters are identical to the DMap.Get,
// but it returns FutureGet to read the batched response.
func (dp *DMapPipeline) Get(ctx context.Context, key string) *FutureGet {
	cmd := protocol.NewGet(dp.dm.name, key).SetRaw().Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureGet{
		dp:     dp,
		partID: partID,
		index:  index,
		ctx:    dp.ctx,
	}
}

// FutureDelete is used to read the result of a pipelined Delete command.
type FutureDelete struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

// Result returns a response for the pipelined Delete command.
func (f *FutureDelete) Result() (int, error) {
	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() != nil {
			return 0, processProtocolError(cmd.Err())
		}
		return int(cmd.(*redis.Cmd).Val().(int64)), nil
	default:
		return 0, ErrNotReady
	}
}

// Delete queues a Delete command. The parameters are identical to the DMap.Delete,
// but it returns FutureDelete to read the batched response.
func (dp *DMapPipeline) Delete(ctx context.Context, key string) *FutureDelete {
	cmd := protocol.NewDel(dp.dm.name, []string{key}...).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureDelete{
		dp:     dp,
		partID: partID,
		index:  index,
		ctx:    dp.ctx,
	}
}

// FutureExpire is used to read the result of a pipelined Expire command.
type FutureExpire struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

// Result returns a response for the pipelined Expire command.
func (f *FutureExpire) Result() error {
	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		return processProtocolError(cmd.Err())
	default:
		return ErrNotReady
	}
}

// Expire queues an Expire command. The parameters are identical to the DMap.Expire,
// but it returns FutureExpire to read the batched response.
func (dp *DMapPipeline) Expire(ctx context.Context, key string, timeout time.Duration) (*FutureExpire, error) {
	cmd := protocol.NewExpire(dp.dm.name, key, timeout).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureExpire{
		dp:     dp,
		partID: partID,
		index:  index,
		ctx:    dp.ctx,
	}, nil
}

// FutureIncr is used to read the result of a pipelined Incr command.
type FutureIncr struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

// Result returns a response for the pipelined Incr command.
func (f *FutureIncr) Result() (int, error) {
	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() != nil {
			return 0, processProtocolError(cmd.Err())
		}
		return int(cmd.(*redis.Cmd).Val().(int64)), nil
	default:
		return 0, ErrNotReady
	}
}

// Incr queues an Incr command. The parameters are identical to the DMap.Incr,
// but it returns FutureIncr to read the batched response.
func (dp *DMapPipeline) Incr(ctx context.Context, key string, delta int) (*FutureIncr, error) {
	cmd := protocol.NewIncr(dp.dm.name, key, delta).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureIncr{
		dp:     dp,
		partID: partID,
		index:  index,
		ctx:    dp.ctx,
	}, nil
}

// FutureDecr is used to read the result of a pipelined Decr command.
type FutureDecr struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

// Result returns a response for the pipelined Decr command.
func (f *FutureDecr) Result() (int, error) {
	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() != nil {
			return 0, processProtocolError(cmd.Err())
		}
		return int(cmd.(*redis.Cmd).Val().(int64)), nil
	default:
		return 0, ErrNotReady
	}
}

// Decr queues a Decr command. The parameters are identical to the DMap.Decr,
// but it returns FutureDecr to read the batched response.
func (dp *DMapPipeline) Decr(ctx context.Context, key string, delta int) (*FutureDecr, error) {
	cmd := protocol.NewDecr(dp.dm.name, key, delta).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureDecr{
		dp:     dp,
		partID: partID,
		index:  index,
		ctx:    dp.ctx,
	}, nil
}

// FutureGetPut is used to read the result of a pipelined GetPut command.
type FutureGetPut struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

// Result returns a response for the pipelined GetPut command.
func (f *FutureGetPut) Result() (*GetResponse, error) {
	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() == redis.Nil {
			// This should be the first run.
			return nil, nil
		}
		if cmd.Err() != nil {
			return nil, processProtocolError(cmd.Err())
		}
		stringCmd := redis.NewStringCmd(context.Background(), cmd.Args()...)
		stringCmd.SetVal(cmd.(*redis.Cmd).Val().(string))
		return f.dp.dm.makeGetResponse(stringCmd)
	default:
		return nil, ErrNotReady
	}
}

// GetPut queues a GetPut command. The parameters are identical to the DMap.GetPut,
// but it returns FutureGetPut to read the batched response.
func (dp *DMapPipeline) GetPut(ctx context.Context, key string, value interface{}) (*FutureGetPut, error) {
	buf := bytes.NewBuffer(nil)

	enc := resp.New(buf)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}

	cmd := protocol.NewGetPut(dp.dm.name, key, buf.Bytes()).SetRaw().Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureGetPut{
		dp:     dp,
		partID: partID,
		index:  index,
		ctx:    dp.ctx,
	}, nil
}

// FutureIncrByFloat is used to read the result of a pipelined IncrByFloat command.
type FutureIncrByFloat struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

// Result returns a response for the pipelined IncrByFloat command.
func (f *FutureIncrByFloat) Result() (float64, error) {
	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() != nil {
			return 0, processProtocolError(cmd.Err())
		}
		stringRes := cmd.(*redis.Cmd).Val().(string)
		return strconv.ParseFloat(stringRes, 64)
	default:
		return 0, ErrNotReady
	}
}

// IncrByFloat queues an IncrByFloat command. The parameters are identical to the DMap.IncrByFloat,
// but it returns FutureIncrByFloat to read the batched response.
func (dp *DMapPipeline) IncrByFloat(ctx context.Context, key string, delta float64) (*FutureIncrByFloat, error) {
	cmd := protocol.NewIncrByFloat(dp.dm.name, key, delta).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureIncrByFloat{
		dp:     dp,
		partID: partID,
		index:  index,
		ctx:    dp.ctx,
	}, nil
}

func (dp *DMapPipeline) execOnPartition(ctx context.Context, partID uint64) error {
	rc, err := dp.dm.clusterClient.clientByPartID(partID)
	if err != nil {
		return err
	}
	commands := dp.commands[partID]
	pipe := rc.Pipeline()

	for _, cmd := range commands {
		pipe.Do(ctx, cmd.Args()...)
	}

	// Exec executes all previously queued commands using one
	// client-server roundtrip.
	//
	// Exec always returns list of commands and error of the first failed
	// command if any.
	result, _ := pipe.Exec(ctx)
	dp.result[partID] = result
	return nil
}

// Exec executes all queued commands using one client-server roundtrip.
func (dp *DMapPipeline) Exec(ctx context.Context) error {
	select {
	case <-dp.ctx.Done():
		return ErrPipelineClosed
	default:
	}

	dp.mtx.Lock()
	defer dp.mtx.Unlock()

	defer dp.cancel()

	var errGr errgroup.Group
	numCpu := 1
	sem := semaphore.NewWeighted(int64(numCpu))
	for i := uint64(0); i < dp.dm.clusterClient.partitionCount; i++ {
		_ = sem.Acquire(ctx, 1)

		partID := i
		errGr.Go(func() error {
			defer sem.Release(1)
			// If execOnPartition returns an error, it will eventually stop
			// all flush operation.
			return dp.execOnPartition(ctx, partID)
		})
	}

	return errGr.Wait()
}

// Discard discards the pipelined commands and resets all internal states.
// A pipeline can be reused after calling Discard.
func (dp *DMapPipeline) Discard() error {
	select {
	case <-dp.ctx.Done():
		return ErrPipelineClosed
	default:
	}

	dp.cancel()

	dp.mtx.Lock()
	defer dp.mtx.Unlock()

	dp.commands = make(map[uint64][]redis.Cmder)
	dp.result = make(map[uint64][]redis.Cmder)
	dp.ctx, dp.cancel = context.WithCancel(context.Background())
	return nil
}

// Close closes the pipeline and frees the allocated resources. You shouldn't try to
// reuse a closed pipeline.
func (dp *DMapPipeline) Close() {
	dp.cancel()
}

// Pipeline is a mechanism to realise Redis Pipeline technique.
//
// Pipelining is a technique to extremely speed up processing by packing
// operations to batches, send them at once to Redis and read a replies in a
// singe step.
// See https://redis.io/topics/pipelining
//
// Pay attention, that Pipeline is not a transaction, so you can get unexpected
// results in case of big pipelines and small read/write timeouts.
// Redis client has retransmission logic in case of timeouts, pipeline
// can be retransmitted and commands can be executed more than once.
func (dm *ClusterDMap) Pipeline() (*DMapPipeline, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &DMapPipeline{
		dm:       dm,
		commands: make(map[uint64][]redis.Cmder),
		result:   make(map[uint64][]redis.Cmder),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}
