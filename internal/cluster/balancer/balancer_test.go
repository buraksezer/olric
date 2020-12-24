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

package balancer

import (
	"context"
	"fmt"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routing_table"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/internal/transport"
	"golang.org/x/sync/errgroup"
	"net"
	"strconv"
	"testing"
)

func newBalancerForTest(c *config.Config, srv *transport.Server) *Balancer {
	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("client", transport.NewClient(c.Client))

	rt := routing_table.New(e)
	if srv != nil {
		ops := map[protocol.OpCode]func(w, r protocol.EncodeDecoder){
			protocol.OpUpdateRouting: rt.UpdateRoutingOperation,
			protocol.OpLengthOfPart:  rt.KeyCountOnPartOperation,
		}
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
		<-srv.StartedCtx.Done()
	}
	e.Set("routingTable", rt)
	b := New(e)
	rt.AddCallback(b.Balance)
	return b
}

type testCluster struct {
	peerPorts []int
	errGr     errgroup.Group
	ctx       context.Context
	cancel    context.CancelFunc
}

func newTestCluster() *testCluster {
	ctx, cancel := context.WithCancel(context.Background())
	return &testCluster{
		ctx: ctx,
		cancel: cancel,
	}
}

func (t *testCluster) addNode(c *config.Config) (*routing_table.RoutingTable, error) {
	if c == nil {
		c = testutil.NewConfig()
	}
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

	srv := testutil.NewTransportServer(c)
	b := newBalancerForTest(c, srv)
	err = b.rt.Start()
	if err != nil {
		return nil, err
	}

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return srv.Shutdown(context.Background())
	})

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return b.rt.Shutdown(context.Background())
	})

	t.peerPorts = append(t.peerPorts, port)
	return rt, err
}

func (t *testCluster) shutdown() error {
	t.cancel()
	return t.errGr.Wait()
}

func TestBalance_Merge(t *testing.T) {
	b := New()
}
