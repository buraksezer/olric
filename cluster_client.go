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
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/kvstore/entry"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/resp"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/buraksezer/olric/stats"
	"github.com/go-redis/redis/v8"
)

var pool = bufpool.New()

type ClusterLockContext struct {
	key   string
	token string
	dm    *ClusterDMap
}

type ClusterDMap struct {
	name          string
	newEntry      func() storage.Entry
	config        *dmapConfig
	engine        storage.Entry
	client        *server.Client
	clusterClient *ClusterClient
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

	enc := resp.New(valueBuf)
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

	e := dm.newEntry()
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

	enc := resp.New(valueBuf)
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

	e := dm.newEntry()
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

func (c *ClusterLockContext) Lease(ctx context.Context, duration time.Duration) error {
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
	var sc dmap.ScanConfig
	for _, opt := range options {
		opt(&sc)
	}
	if sc.Count == 0 {
		sc.Count = DefaultScanCount
	}

	ictx, cancel := context.WithCancel(ctx)
	i := &ClusterIterator{
		dm:            dm,
		clusterClient: dm.clusterClient,
		config:        &sc,
		logger:        dm.clusterClient.logger,
		allKeys:       make(map[string]struct{}),
		finished:      make(map[string]struct{}),
		cursors:       make(map[string]uint64),
		ctx:           ictx,
		cancel:        cancel,
	}

	if err := i.fetchRoutingTable(); err != nil {
		return nil, err
	}

	i.wg.Add(1)
	go i.fetchRoutingTablePeriodically()

	return i, nil
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
	config *clusterClientConfig
	logger *log.Logger
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

func (cl *ClusterClient) Stats(ctx context.Context, options ...StatsOption) (stats.Stats, error) {
	var cfg statsConfig
	for _, opt := range options {
		opt(&cfg)
	}

	statsCmd := protocol.NewStats()
	if cfg.CollectRuntime {
		statsCmd.SetCollectRuntime()
	}

	cmd := statsCmd.Command(ctx)
	rc, err := cl.client.Pick()
	if err != nil {
		return stats.Stats{}, err
	}

	err = rc.Process(ctx, cmd)
	if err != nil {
		return stats.Stats{}, processProtocolError(err)
	}

	if err = cmd.Err(); err != nil {
		return stats.Stats{}, processProtocolError(err)
	}
	data, err := cmd.Bytes()
	if err != nil {
		return stats.Stats{}, processProtocolError(err)
	}
	var s stats.Stats
	err = json.Unmarshal(data, &s)
	if err != nil {
		return stats.Stats{}, processProtocolError(err)
	}
	return s, nil
}

func (cl *ClusterClient) Members(ctx context.Context) ([]Member, error) {
	rc, err := cl.client.Pick()
	if err != nil {
		return []Member{}, err
	}

	cmd := protocol.NewClusterMembers().Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return []Member{}, processProtocolError(err)
	}

	if err = cmd.Err(); err != nil {
		return []Member{}, processProtocolError(err)
	}

	items, err := cmd.Slice()
	if err != nil {
		return []Member{}, processProtocolError(err)
	}
	var members []Member
	for _, rawItem := range items {
		m := Member{}
		item := rawItem.([]interface{})
		m.Name = item[0].(string)

		switch id := item[1].(type) {
		case uint64:
			m.ID = id
		case int64:
			m.ID = uint64(id)
		}

		m.Birthdate = item[2].(int64)
		if item[3] == "true" {
			m.Coordinator = true
		}
		members = append(members, m)
	}
	return members, nil
}

func (cl *ClusterClient) Close(ctx context.Context) error {
	return cl.client.Shutdown(ctx)
}

func (cl *ClusterClient) NewPubSub(options ...PubSubOption) (*PubSub, error) {
	return newPubSub(cl.client, options...)
}

func (cl *ClusterClient) NewDMap(name string, options ...DMapOption) (DMap, error) {
	var dc dmapConfig
	for _, opt := range options {
		opt(&dc)
	}

	if dc.storageEntryImplementation == nil {
		dc.storageEntryImplementation = func() storage.Entry {
			return entry.New()
		}
	}

	return &ClusterDMap{name: name,
		config:        &dc,
		newEntry:      dc.storageEntryImplementation,
		client:        cl.client,
		clusterClient: cl,
	}, nil
}

type ClusterClientOption func(c *clusterClientConfig)

type clusterClientConfig struct {
	logger *log.Logger
	config *config.Client
}

func WithLogger(l *log.Logger) ClusterClientOption {
	return func(cfg *clusterClientConfig) {
		cfg.logger = l
	}
}

func WithConfig(c *config.Client) ClusterClientOption {
	return func(cfg *clusterClientConfig) {
		cfg.config = c
	}
}

func NewClusterClient(addresses []string, options ...ClusterClientOption) (*ClusterClient, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("addresses cannot be empty")
	}

	var cc clusterClientConfig
	for _, opt := range options {
		opt(&cc)
	}

	if cc.logger == nil {
		cc.logger = log.New(os.Stderr, "logger: ", log.Lshortfile)
	}

	if cc.config == nil {
		cc.config = config.NewClient()
	}

	if err := cc.config.Sanitize(); err != nil {
		return nil, err
	}
	if err := cc.config.Validate(); err != nil {
		return nil, err
	}

	cl := &ClusterClient{
		client: server.NewClient(cc.config),
		config: &cc,
		logger: cc.logger,
	}
	for _, address := range addresses {
		cl.client.Get(address)
	}
	return cl, nil
}

var (
	_ Client = (*ClusterClient)(nil)
	_ DMap   = (*ClusterDMap)(nil)
)
