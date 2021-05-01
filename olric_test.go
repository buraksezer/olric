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

package olric

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/hashicorp/memberlist"
)

func newTestOlric(t *testing.T) (*Olric, error) {
	c := testutil.NewConfig()
	port, err := testutil.GetFreePort()
	if err != nil {
		return nil, err
	}
	if c.MemberlistConfig == nil {
		c.MemberlistConfig = memberlist.DefaultLocalConfig()
	}
	c.MemberlistConfig.BindPort = 0

	c.BindAddr = "127.0.0.1"
	c.BindPort = port

	err = c.Sanitize()
	if err != nil {
		return nil, err
	}
	err = c.Validate()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		cancel()
	}

	db, err := New(c)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := db.Start(); err != nil {
			panic(fmt.Sprintf("Failed to run Olric: %v", err))
		}
	}()

	select {
	case <-time.After(time.Second):
		t.Fatalf("Olric cannot be started in one second")
	case <-ctx.Done():
		// everything is fine
	}
	t.Cleanup(func() {
		if err := db.Shutdown(context.Background()); err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	})
	return db, nil
}

func TestOlric_StartAndShutdown(t *testing.T) {
	db, err := newTestOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = db.Shutdown(context.Background())
	if err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
	}
}
