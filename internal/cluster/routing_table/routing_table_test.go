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

package routing_table

import (
	"context"
	"fmt"
	"testing"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/internal/transport"
)

func newRoutingTableForTest(c *config.Config, srv *transport.Server) *RoutingTable {
	flogger := testutil.NewFlogger(c)
	primary := partitions.New(c.PartitionCount, partitions.PRIMARY)
	backup := partitions.New(c.PartitionCount, partitions.BACKUP)
	client := transport.NewClient(c.Client)
	rt := New(c, flogger, primary, backup, client)

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
	return rt
}

func TestRoutingTable_Start(t *testing.T) {
	c := testutil.NewConfig()
	srv := testutil.NewTransportServer(c)
	defer func() {
		if err := srv.Shutdown(context.Background()); err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	rt := newRoutingTableForTest(c, srv)
	err := rt.Start()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	defer func() {
		if err := rt.Shutdown(context.Background()); err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()
}
