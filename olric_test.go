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
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
)

func newTestOlric(t *testing.T) *Olric {
	c := testutil.NewConfig()
	port, err := testutil.GetFreePort()
	require.NoError(t, err)

	if c.MemberlistConfig == nil {
		c.MemberlistConfig = memberlist.DefaultLocalConfig()
	}
	c.MemberlistConfig.BindPort = 0

	c.BindAddr = "127.0.0.1"
	c.BindPort = port

	err = c.Sanitize()
	require.NoError(t, err)

	err = c.Validate()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		cancel()
	}

	db, err := New(c)
	require.NoError(t, err)

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
		serr := db.Shutdown(context.Background())
		require.NoError(t, serr)
	})

	return db
}

func TestOlric_StartAndShutdown(t *testing.T) {
	db := newTestOlric(t)
	err := db.Shutdown(context.Background())
	require.NoError(t, err)
}
