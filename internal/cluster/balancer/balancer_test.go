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

package balancer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routingtable"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/internal/testutil/mockfragment"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func newTestEnvironment(c *config.Config) *environment.Environment {
	if c == nil {
		c = testutil.NewConfig()
	}

	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("client", transport.NewClient(c.Client))
	return e
}

func newBalancerForTest(e *environment.Environment, srv *transport.Server) *Balancer {
	rt := routingtable.New(e)
	if srv != nil {
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
		<-srv.StartedCtx.Done()
	}
	e.Set("routingtable", rt)
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
		ctx:    ctx,
		cancel: cancel,
	}
}

func (t *testCluster) addNode(e *environment.Environment) (*Balancer, error) {
	if e == nil {
		e = newTestEnvironment(nil)
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

	srv := testutil.NewTransportServer(c)
	b := newBalancerForTest(e, srv)
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
	return b, err
}

func (t *testCluster) shutdown() error {
	t.cancel()
	return t.errGr.Wait()
}

func insertRandomData(e *environment.Environment, kind partitions.Kind) int {
	var total int
	c := e.Get("config").(*config.Config)
	part := e.Get(strings.ToLower(kind.String())).(*partitions.Partitions)
	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		part := part.PartitionByID(partID)
		s := mockfragment.New()
		s.Fill()
		part.Map().Store("test-data", s)
		total += part.Length()
	}
	return total
}

func checkKeyCountAfterBalance(e *environment.Environment, kind partitions.Kind, total int) error {
	c := e.Get("config").(*config.Config)
	part := e.Get(strings.ToLower(kind.String())).(*partitions.Partitions)
	var afterBalance int
	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		part := part.PartitionByID(partID)
		afterBalance += part.Length()
	}
	if afterBalance == total {
		return fmt.Errorf("node still has the same data set")
	}
	return nil
}

func checkBackupOwnership(e *environment.Environment) error {
	c := e.Get("config").(*config.Config)
	primary := e.Get(strings.ToLower(partitions.PRIMARY.String())).(*partitions.Partitions)
	backup := e.Get(strings.ToLower(partitions.BACKUP.String())).(*partitions.Partitions)
	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		primaryOwner := primary.PartitionByID(partID).Owner()
		part := backup.PartitionByID(partID)
		for _, owner := range part.Owners() {
			if primaryOwner.CompareByID(owner) {
				return fmt.Errorf("%s is the primary and backup owner of partID: %d at the same time", primaryOwner, partID)
			}
		}
	}
	return nil
}

func TestBalance_Move(t *testing.T) {
	cluster := newTestCluster()
	defer func() {
		require.NoError(t, cluster.shutdown())
	}()

	e1 := newTestEnvironment(nil)
	b1, err := cluster.addNode(e1)
	require.NoError(t, err)

	defer b1.Shutdown()
	keyCountOnNode1 := insertRandomData(e1, partitions.PRIMARY)

	e2 := newTestEnvironment(nil)
	b2, err := cluster.addNode(e2)
	require.NoError(t, err)

	defer b2.Shutdown()

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	keyCountOnNode2 := insertRandomData(e1, partitions.PRIMARY)

	b1.Balance()
	b2.Balance()

	err = checkKeyCountAfterBalance(e1, partitions.PRIMARY, keyCountOnNode1)
	require.NoError(t, err)

	err = checkKeyCountAfterBalance(e2, partitions.PRIMARY, keyCountOnNode2)
	require.NoError(t, err)
}

func TestBalance_Backup_Move(t *testing.T) {
	cluster := newTestCluster()
	defer func() {
		require.NoError(t, cluster.shutdown())
	}()

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	e1 := newTestEnvironment(c1)
	b1, err := cluster.addNode(e1)
	require.NoError(t, err)

	defer b1.Shutdown()
	b1.rt.UpdateEagerly()

	err = checkBackupOwnership(e1)
	require.NoError(t, err)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	e2 := newTestEnvironment(c2)
	b2, err := cluster.addNode(e2)
	require.NoError(t, err)

	defer b2.Shutdown()

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	b1.rt.UpdateEagerly()

	insertRandomData(e1, partitions.BACKUP)

	err = checkBackupOwnership(e2)
	require.NoError(t, err)

	c3 := testutil.NewConfig()
	c3.ReplicaCount = 2
	e3 := newTestEnvironment(c3)
	b3, err := cluster.addNode(e3)
	require.NoError(t, err)

	defer b3.Shutdown()

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b3.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	b1.rt.UpdateEagerly()
	// Call second time to clear the table.
	b1.rt.UpdateEagerly()

	err = checkBackupOwnership(e3)
	require.NoError(t, err)
}
