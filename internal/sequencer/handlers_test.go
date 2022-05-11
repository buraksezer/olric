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

package sequencer

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestSequencer_ReadVersion_CommitVersion(t *testing.T) {
	c := makeSequencerConfig(t)

	lg := log.New(os.Stderr, "", 0)
	sq, err := New(c, lg)
	require.NoError(t, err)

	errCh := make(chan error)
	go func() {
		errCh <- sq.Start()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rc := newRedisClient(t, c)
	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		cmd := rc.Ping(ctx)
		return cmd.Err()
	})
	require.NoError(t, err)

	t.Run("Call SEQUENCER.READVERSION", func(t *testing.T) {
		cmd := protocol.NewSequencerReadVersion().Command(ctx)
		require.NoError(t, rc.Process(ctx, cmd))
		readVersion, err := cmd.Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), readVersion)
	})

	t.Run("Call SEQUENCER.COMMITVERSION", func(t *testing.T) {
		cmd := protocol.NewSequencerCommitVersion().Command(ctx)
		require.NoError(t, rc.Process(ctx, cmd))
		commitVersion, err := cmd.Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), commitVersion)
	})

	t.Run("Call SEQUENCER.COMMITVERSION concurrently", func(t *testing.T) {
		cmd := protocol.NewSequencerReadVersion().Command(ctx)
		require.NoError(t, rc.Process(ctx, cmd))
		readVersion, err := cmd.Result()
		require.NoError(t, err)

		var wg sync.WaitGroup

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cmd := protocol.NewSequencerCommitVersion().Command(ctx)
				require.NoError(t, rc.Process(ctx, cmd))
				_, err := cmd.Result()
				require.NoError(t, err)
			}()
		}
		wg.Wait()

		cmd = protocol.NewSequencerReadVersion().Command(ctx)
		require.NoError(t, rc.Process(ctx, cmd))
		res, err := cmd.Result()
		require.NoError(t, err)
		require.Equal(t, readVersion+100, res)

	})

	require.NoError(t, sq.Shutdown(ctx))
	require.NoError(t, <-errCh)
}
