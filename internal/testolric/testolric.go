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

package testolric

import (
	"context"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/kvstore"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
)

type TestOlric struct {
	db   *olric.Olric
	Addr string
}

func (o *TestOlric) Olric() *olric.Olric {
	return o.db
}

func New(t *testing.T) (*TestOlric, error) {
	port, err := testutil.GetFreePort()
	if err != nil {
		return nil, err
	}
	sc := config.NewStorageEngine()
	// default storage engine: kvstore
	engine := &kvstore.KVStore{}
	sc.Config[engine.Name()] = map[string]interface{}{
		"tableSize": 102134,
	}
	sc.Impls[engine.Name()] = engine

	mc := memberlist.DefaultLocalConfig()
	mc.BindAddr = "127.0.0.1"
	mc.BindPort = 0

	cfg := config.New("local")
	cfg.BindAddr = "127.0.0.1"
	cfg.BindPort = port
	cfg.StorageEngines = sc
	cfg.MemberlistConfig = mc
	cfg.PartitionCount = 7

	ctx, cancel := context.WithCancel(context.Background())
	cfg.Started = func() {
		cancel()
	}
	db, err := olric.New(cfg)
	if err != nil {
		return nil, err
	}

	srv := &TestOlric{
		db:   db,
		Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
	}
	var startError error
	go func() {
		startError = db.Start()
		if startError != nil {
			log.Printf("[ERROR] olric.Start returned an error: %v", startError)
		}
	}()

	select {
	case <-time.After(time.Second):
		return nil, errors.Wrap(startError, "olric node cannot be started in 1 second")
	case <-ctx.Done():
	}

	t.Cleanup(func() {
		shutdownErr := db.Shutdown(context.Background())
		if shutdownErr != nil {
			t.Errorf("Expected nil. Got %v", shutdownErr)
		}
	})
	return srv, nil
}
