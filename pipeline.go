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

var ErrNotReady = errors.New("not ready yet")

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

type FuturePut struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

func (f *FuturePut) Result() error {
	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		return processProtocolError(cmd.Err())
	default:
		return ErrNotReady
	}
}

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

type FutureGet struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

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

type FutureDelete struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

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

type FutureExpire struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

func (f *FutureExpire) Result() error {
	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		return processProtocolError(cmd.Err())
	default:
		return ErrNotReady
	}
}

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

type FutureIncr struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

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

type FutureDecr struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

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

type FutureGetPut struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

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

type FutureIncrByFloat struct {
	dp     *DMapPipeline
	partID uint64
	index  int
	ctx    context.Context
}

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

func (dp *DMapPipeline) flushOnPartition(ctx context.Context, partID uint64) error {
	rc, err := dp.dm.clusterClient.clientByPartID(partID)
	if err != nil {
		return err
	}
	commands := dp.commands[partID]
	pipe := rc.Pipeline()

	for _, cmd := range commands {
		pipe.Do(ctx, cmd.Args()...)
	}

	result, _ := pipe.Exec(ctx)
	dp.result[partID] = result
	return nil
}

func (dp *DMapPipeline) Flush(ctx context.Context) error {
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
			return dp.flushOnPartition(ctx, partID)
		})
	}

	return errGr.Wait()
}

func (dm *ClusterDMap) Pipeline() *DMapPipeline {
	ctx, cancel := context.WithCancel(context.Background())
	return &DMapPipeline{
		dm:       dm,
		commands: make(map[uint64][]redis.Cmder),
		result:   make(map[uint64][]redis.Cmder),
		ctx:      ctx,
		cancel:   cancel,
	}
}
