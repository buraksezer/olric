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

package testcluster

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routing_table"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/locker"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/service"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/internal/transport"
	"golang.org/x/sync/errgroup"
)

type TestCluster struct {
	peerPorts   []int
	constructor func(e *environment.Environment) (service.Service, error)
	errGr       errgroup.Group
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewEnvironment(c *config.Config) *environment.Environment {
	if c == nil {
		c = testutil.NewConfig()
	}

	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("client", transport.NewClient(c.Client))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("locker", locker.New())
	return e
}

func (t *TestCluster) newService(e *environment.Environment) service.Service {
	c := e.Get("config").(*config.Config)
	srv := testutil.NewTransportServer(c)

	rt := routing_table.New(e)
	e.Set("routingTable", rt)
	ops := make(map[protocol.OpCode]func(w, r protocol.EncodeDecoder))
	rt.RegisterOperations(ops)

	requestDispatcher := func(w, r protocol.EncodeDecoder) {
		f := ops[r.OpCode()]
		f(w, r)
	}
	srv.SetDispatcher(requestDispatcher)
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			panic(fmt.Sprintf("ListenAndServe returned an error: %v", err))
		}
	}()
	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return srv.Shutdown(context.Background())
	})
	<-srv.StartedCtx.Done()

	s, err := t.constructor(e)
	if err != nil {
		panic(fmt.Sprintf("failed to start DMap service: %v", err))
	}
	s.RegisterOperations(ops)
	return s
}

func New(constructor func(e *environment.Environment) (service.Service, error)) *TestCluster {
	ctx, cancel := context.WithCancel(context.Background())
	return &TestCluster{
		constructor: constructor,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (t *TestCluster) AddNode(e *environment.Environment) (service.Service, error) {
	if e == nil {
		e = NewEnvironment(nil)
	}
	c := e.Get("config").(*config.Config)

	port, err := testutil.GetFreePort()
	if err != nil {
		return nil, err
	}
	c.MemberlistConfig.BindPort = port

	var peers []string
	for _, peerPort := range t.peerPorts {
		peers = append(peers, net.JoinHostPort("127.0.0.1", strconv.Itoa(peerPort)))
	}
	c.Peers = peers

	s := t.newService(e)

	rt := e.Get("routingTable").(*routing_table.RoutingTable)
	err = rt.Start()
	if err != nil {
		return nil, err
	}

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return rt.Shutdown(context.Background())
	})

	t.errGr.Go(func() error {
		return s.Start()
	})

	t.peerPorts = append(t.peerPorts, port)
	return s, err
}

func (t *TestCluster) Shutdown() error {
	t.cancel()
	return t.errGr.Wait()
}
