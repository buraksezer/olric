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
	"context"
	"github.com/go-redis/redis/v8"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/encoding"
	"github.com/buraksezer/olric/internal/kvstore/entry"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/pkg/storage"
)

var pool = bufpool.New()

type ClusterLockContext struct {
	key   string
	token string
	dm    *ClusterDMap
}

type ClusterDMap struct {
	name   string
	config *dmapConfig
	engine storage.Entry
	client *server.Client
}

func (dm *ClusterDMap) Name() string {
	return dm.name
}

func processProtocolError(err error) error {
	if err == nil {
		return nil
	}
	if err == redis.Nil {
		return ErrKeyNotFound
	}
	return convertDMapError(protocol.ConvertError(err))
}

func (dm *ClusterDMap) writePutCommand(c *dmap.PutConfig, key string, value []byte) *protocol.Put {
	cmd := protocol.NewPut(dm.name, key, value)
	switch {
	case c.HasEX:
		cmd.SetEX(c.EX.Seconds())
	case c.HasPX:
		cmd.SetPX(c.PX.Milliseconds())
	case c.HasEXAT:
		cmd.SetEXAT(c.EXAT.Seconds())
	case c.HasPXAT:
		cmd.SetPXAT(c.PXAT.Milliseconds())
	}

	switch {
	case c.HasNX:
		cmd.SetNX()
	case c.HasXX:
		cmd.SetXX()
	}

	return cmd
}

func (dm *ClusterDMap) Put(ctx context.Context, key string, value interface{}, options ...PutOption) error {
	rc, err := dm.client.Pick()
	if err != nil {
		return err
	}

	valueBuf := pool.Get()
	defer pool.Put(valueBuf)

	enc := encoding.New(valueBuf)
	err = enc.Encode(value)
	if err != nil {
		return err
	}

	var pc dmap.PutConfig
	for _, opt := range options {
		opt(&pc)
	}
	putCmd := dm.writePutCommand(&pc, key, valueBuf.Bytes())
	cmd := putCmd.Command(ctx)

	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}
	return processProtocolError(cmd.Err())
}

func (dm *ClusterDMap) Get(ctx context.Context, key string) (*GetResponse, error) {
	rc, err := dm.client.Pick()
	if err != nil {
		return nil, err
	}

	cmd := protocol.NewGet(dm.name, key).SetRaw().Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return nil, processProtocolError(err)
	}

	raw, err := cmd.Bytes()
	if err != nil {
		return nil, processProtocolError(err)
	}

	// TODO: We have to create a new entry with a callback function
	e := entry.New()
	e.Decode(raw)
	return &GetResponse{
		entry: e,
	}, nil
}

func (dm *ClusterDMap) Delete(ctx context.Context, key string) error {
	rc, err := dm.client.Pick()
	if err != nil {
		return err
	}

	cmd := protocol.NewDel(dm.name, key).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}

	return processProtocolError(cmd.Err())
}

func (dm *ClusterDMap) Incr(ctx context.Context, key string, delta int) (int, error) {
	rc, err := dm.client.Pick()
	if err != nil {
		return 0, err
	}

	cmd := protocol.NewIncr(dm.name, key, delta).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return 0, processProtocolError(err)
	}
	// TODO: Consider returning uint64 as response
	res, err := cmd.Uint64()
	if err != nil {
		return 0, processProtocolError(cmd.Err())
	}
	return int(res), nil
}

func (dm *ClusterDMap) Decr(ctx context.Context, key string, delta int) (int, error) {
	rc, err := dm.client.Pick()
	if err != nil {
		return 0, err
	}

	cmd := protocol.NewDecr(dm.name, key, delta).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return 0, processProtocolError(err)
	}
	// TODO: Consider returning uint64 as response
	res, err := cmd.Uint64()
	if err != nil {
		return 0, processProtocolError(cmd.Err())
	}
	return int(res), nil
}

func (dm *ClusterDMap) GetPut(ctx context.Context, key string, value interface{}) (*GetResponse, error) {
	rc, err := dm.client.Pick()
	if err != nil {
		return nil, err
	}

	valueBuf := pool.Get()
	defer pool.Put(valueBuf)

	enc := encoding.New(valueBuf)
	err = enc.Encode(value)
	if err != nil {
		return nil, err
	}

	cmd := protocol.NewGetPut(dm.name, key, valueBuf.Bytes()).SetRaw().Command(ctx)
	err = rc.Process(ctx, cmd)
	err = processProtocolError(err)
	if err != nil {
		// First try to set a key/value with GetPut
		if err == ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	raw, err := cmd.Bytes()
	if err != nil {
		return nil, processProtocolError(err)
	}

	// TODO: We have to create a new entry with a callback function
	e := entry.New()
	e.Decode(raw)
	return &GetResponse{
		entry: e,
	}, nil
}

func (dm *ClusterDMap) Expire(ctx context.Context, key string, timeout time.Duration) error {
	rc, err := dm.client.Pick()
	if err != nil {
		return err
	}

	cmd := protocol.NewExpire(dm.name, key, timeout).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}
	return processProtocolError(cmd.Err())
}

func (dm *ClusterDMap) Lock(ctx context.Context, key string, deadline time.Duration) (LockContext, error) {
	rc, err := dm.client.Pick()
	if err != nil {
		return nil, err
	}

	// TODO: Inconsistency: TIMEOUT, duration or second?
	cmd := protocol.NewLock(dm.name, key, deadline.Seconds()).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return nil, processProtocolError(err)
	}

	token, err := cmd.Bytes()
	if err != nil {
		return nil, processProtocolError(err)
	}
	return &ClusterLockContext{
		key:   key,
		token: string(token),
		dm:    dm,
	}, nil
}

func (dm *ClusterDMap) LockWithTimeout(ctx context.Context, key string, timeout, deadline time.Duration) (LockContext, error) {
	rc, err := dm.client.Pick()
	if err != nil {
		return nil, err
	}

	// TODO: Inconsistency: TIMEOUT, duration or second?
	cmd := protocol.NewLock(dm.name, key, deadline.Seconds()).SetPX(timeout.Milliseconds()).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return nil, processProtocolError(err)
	}

	token, err := cmd.Bytes()
	if err != nil {
		return nil, processProtocolError(err)
	}

	return &ClusterLockContext{
		key:   key,
		token: string(token),
		dm:    dm,
	}, nil
}

func (c *ClusterLockContext) Unlock(ctx context.Context) error {
	rc, err := c.dm.client.Pick()
	if err != nil {
		return err
	}
	cmd := protocol.NewUnlock(c.dm.name, c.key, c.token).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}
	return processProtocolError(cmd.Err())
}

func (c ClusterLockContext) Lease(ctx context.Context, duration time.Duration) error {
	rc, err := c.dm.client.Pick()
	if err != nil {
		return err
	}
	// TODO: Inconsistency!
	cmd := protocol.NewLockLease(c.dm.name, c.key, c.token, duration.Seconds()).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}
	return processProtocolError(cmd.Err())
}

func (dm *ClusterDMap) Scan(ctx context.Context, options ...ScanOption) (Iterator, error) {
	//TODO implement me
	panic("implement me")
}

func (dm *ClusterDMap) Destroy(ctx context.Context) error {
	rc, err := dm.client.Pick()
	if err != nil {
		return err
	}

	cmd := protocol.NewDestroy(dm.name).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}

	return processProtocolError(cmd.Err())
}

type ClusterClient struct {
	client *server.Client
}

func NewClusterClient(addresses []string, c *config.Client) (*ClusterClient, error) {
	if c == nil {
		c = config.NewClient()
	}

	if err := c.Sanitize(); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}

	cl := &ClusterClient{
		client: server.NewClient(c),
	}
	for _, address := range addresses {
		cl.client.Get(address)
	}
	return cl, nil
}

func (cl *ClusterClient) NewDMap(name string, options ...DMapOption) (DMap, error) {
	return &ClusterDMap{name: name,
		client: cl.client,
	}, nil
}

func (cl *ClusterClient) Ping(ctx context.Context, addr string) error {
	cmd := protocol.NewPing().Command(ctx)
	rc := cl.client.Get(addr)
	err := rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}
	return processProtocolError(cmd.Err())

}

func (cl *ClusterClient) PingWithMessage(ctx context.Context, addr, message string) (string, error) {
	cmd := protocol.NewPing().SetMessage(message).Command(ctx)
	rc := cl.client.Get(addr)
	err := rc.Process(ctx, cmd)
	if err != nil {
		return "", processProtocolError(err)

	}
	if err = cmd.Err(); err != nil {
		return "", processProtocolError(err)

	}
	res, err := cmd.Bytes()
	if err != nil {
		return "", processProtocolError(err)
	}
	return string(res), nil
}

func (cl *ClusterClient) RoutingTable(ctx context.Context) (RoutingTable, error) {
	cmd := protocol.NewClusterRoutingTable().Command(ctx)
	rc, err := cl.client.Pick()
	if err != nil {
		return RoutingTable{}, err
	}

	err = rc.Process(ctx, cmd)
	if err != nil {
		return RoutingTable{}, processProtocolError(err)
	}

	if err = cmd.Err(); err != nil {
		return RoutingTable{}, processProtocolError(err)
	}

	result, err := cmd.Slice()
	if err != nil {
		return RoutingTable{}, processProtocolError(err)

	}
	return mapToRoutingTable(result)
}

func (cl *ClusterClient) Close(ctx context.Context) error {
	return cl.client.Shutdown(ctx)
}
