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

package transactionlog

import (
	"context"
	"github.com/buraksezer/olric/internal/protocol"
	"log"
	"os"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestTransactionLog_Add(t *testing.T) {
	c := makeResolverConfig(t)

	lg := log.New(os.Stderr, "", 0)
	ts, err := New(c, lg)
	require.NoError(t, err)

	errCh := make(chan error)
	go func() {
		errCh <- ts.Start()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rc := newRedisClient(t, c)
	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		cmd := rc.Ping(ctx)
		return cmd.Err()
	})
	require.NoError(t, err)

	data := []byte("Hello, World!")
	tsAddCmd := protocol.NewTransactionLogAdd(10, data).Command(ctx)
	err = rc.Process(ctx, tsAddCmd)
	require.NoError(t, err)
	require.NoError(t, tsAddCmd.Err())

	require.NoError(t, ts.Shutdown(ctx))
	require.NoError(t, <-errCh)
}
