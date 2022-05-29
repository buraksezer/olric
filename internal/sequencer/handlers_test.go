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

func TestSequencer_Versioning(t *testing.T) {
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

	t.Run("Call SEQUENCER.GETREADVERSION", func(t *testing.T) {
		cmd := protocol.NewSequencerGetReadVersion().Command(ctx)
		require.NoError(t, rc.Process(ctx, cmd))
		readVersion, err := cmd.Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), readVersion)
	})

	t.Run("Call SEQUENCER.GETCOMMITVERSION", func(t *testing.T) {
		cmd := protocol.NewSequencerGetCommitVersion().Command(ctx)
		require.NoError(t, rc.Process(ctx, cmd))
		commitVersion, err := cmd.Result()
		require.NoError(t, err)

		require.Greater(t, commitVersion, int64(0))
	})

	t.Run("Call SEQUENCER.GETCOMMITVERSION concurrently", func(t *testing.T) {

		var wg sync.WaitGroup
		var mtx sync.RWMutex

		keys := make(map[int64]struct{})
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cmd := protocol.NewSequencerGetCommitVersion().Command(ctx)
				require.NoError(t, rc.Process(ctx, cmd))
				commitVersion, err := cmd.Result()
				require.NoError(t, err)

				mtx.Lock()
				keys[commitVersion] = struct{}{}
				mtx.Unlock()
			}()
		}
		wg.Wait()

		mtx.Lock()
		require.Len(t, keys, 100)
		mtx.Unlock()
	})

	t.Run("Call SEQUENCER.UPDATEREADVERSION", func(t *testing.T) {
		commitCmd := protocol.NewSequencerGetCommitVersion().Command(ctx)
		require.NoError(t, rc.Process(ctx, commitCmd))
		commitVersion, err := commitCmd.Result()
		require.NoError(t, err)

		require.Greater(t, commitVersion, int64(0))

		updateCmd := protocol.NewSequencerUpdateReadVersion(commitVersion).Command(ctx)
		require.NoError(t, rc.Process(ctx, updateCmd))
		_, err = updateCmd.Result()
		require.NoError(t, err)

		readCmd := protocol.NewSequencerGetReadVersion().Command(ctx)
		require.NoError(t, rc.Process(ctx, readCmd))
		readVersion, err := readCmd.Result()
		require.NoError(t, err)
		require.Equal(t, commitVersion, readVersion)
	})

	t.Run("Call SEQUENCER.UPDATEREADVERSION - Only Increase", func(t *testing.T) {
		commitCmd := protocol.NewSequencerGetCommitVersion().Command(ctx)
		require.NoError(t, rc.Process(ctx, commitCmd))
		commitVersion, err := commitCmd.Result()
		require.NoError(t, err)

		require.Greater(t, commitVersion, int64(0))

		updateCmd := protocol.NewSequencerUpdateReadVersion(commitVersion).Command(ctx)
		require.NoError(t, rc.Process(ctx, updateCmd))
		_, err = updateCmd.Result()
		require.NoError(t, err)

		for i := int64(0); i < 10; i++ {
			newCommitVersion := commitVersion - i
			updateCmd = protocol.NewSequencerUpdateReadVersion(newCommitVersion).Command(ctx)
			require.NoError(t, rc.Process(ctx, updateCmd))
			_, err = updateCmd.Result()
			require.NoError(t, err)

			readCmd := protocol.NewSequencerGetReadVersion().Command(ctx)
			require.NoError(t, rc.Process(ctx, readCmd))
			readVersion, err := readCmd.Result()
			require.NoError(t, err)
			require.Equal(t, commitVersion, readVersion)
		}
	})

	require.NoError(t, sq.Shutdown(ctx))
	require.NoError(t, <-errCh)
}

func TestSequencer_Restart(t *testing.T) {
	c := makeSequencerConfig(t)

	// Start a sequencer instance
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

	// Get a commit version
	cmd := protocol.NewSequencerGetCommitVersion().Command(ctx)
	require.NoError(t, rc.Process(ctx, cmd))
	commitVersion, err := cmd.Result()
	require.NoError(t, err)

	require.Greater(t, commitVersion, int64(0))

	// Update the read version
	updateCmd := protocol.NewSequencerUpdateReadVersion(commitVersion).Command(ctx)
	require.NoError(t, rc.Process(ctx, updateCmd))
	_, err = updateCmd.Result()
	require.NoError(t, err)

	// Shutdown the sequencer instance
	require.NoError(t, sq.Shutdown(ctx))

	// Start a new sequencer instance
	sq, err = New(c, lg)
	require.NoError(t, err)
	errCh = make(chan error)
	go func() {
		errCh <- sq.Start()
	}()

	rc2 := newRedisClient(t, c)
	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		cmd := rc2.Ping(ctx)
		return cmd.Err()
	})
	require.NoError(t, err)

	// Get a read version.
	readVersionCmd := protocol.NewSequencerGetReadVersion().Command(ctx)
	require.NoError(t, rc2.Process(ctx, readVersionCmd))
	readVersion, err := readVersionCmd.Result()
	require.NoError(t, err)
	require.Equal(t, commitVersion, readVersion)
}
