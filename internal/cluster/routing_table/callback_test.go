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
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/flog"
)

func testRoutingTable() *RoutingTable {
	c := config.New("local")
	flogger := flog.New(c.Logger)
	flogger.SetLevel(c.LogVerbosity)
	if c.LogLevel == "DEBUG" {
		flogger.ShowLineNumber(1)
	}
	primary := partitions.New(7, partitions.PRIMARY)
	backup := partitions.New(7, partitions.BACKUP)
	client := transport.NewClient(c.Client)
	return New(c, flogger, primary, backup, client)
}

func TestRoutingTable_Callback(t *testing.T) {
	rt := testRoutingTable()
	var num int32
	increase := func() {
		atomic.AddInt32(&num, 1)
	}
	rt.AddCallback(increase)
	rt.wg.Add(1)
	go rt.runCallbacks()
	<-time.After(100 * time.Millisecond)
	modified := atomic.LoadInt32(&num)
	if modified != 1 {
		t.Fatalf("Expected number: 1. Got: %v", modified)
	}
}
