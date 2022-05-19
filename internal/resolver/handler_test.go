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

package resolver

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestResolver_Ping(t *testing.T) {
	c := makeResolverConfig(t)

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

	t.Run("Send PING and expect PONG as response", func(t *testing.T) {
		cmd := rc.Ping(ctx)
		res, err := cmd.Result()
		require.Equal(t, olric.DefaultPingResponse, res)
		require.NoError(t, err)
	})

	t.Run("Send PING and expect custom response", func(t *testing.T) {
		cmd := redis.NewStatusCmd(ctx, "PING", "FOOBAR")
		require.NoError(t, rc.Process(ctx, cmd))
		res, err := cmd.Result()
		require.Equal(t, "FOOBAR", res)
		require.NoError(t, err)
	})

	require.NoError(t, sq.Shutdown(ctx))
	require.NoError(t, <-errCh)
}

func commitMessageBuilder(readVersion, commitVersion uint32) *CommitMessage {
	return &CommitMessage{
		ReadVersion:   readVersion,
		CommitVersion: commitVersion,
		Keys: []WrappedKey{
			{
				Key:  "a",
				Kind: ReadCommandKind,
			},
			{
				Key:  "b",
				Kind: ReadCommandKind,
			},
			{
				Key:  "a",
				Kind: MutateCommandKind,
			},
			{
				Key:  "b",
				Kind: MutateCommandKind,
			},
			{
				Key:  "c",
				Kind: MutateCommandKind,
			},
		},
	}
}

func TestResolver_SSI_Commit_Single_Transaction(t *testing.T) {
	c := makeResolverConfig(t)

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

	data, err := msgpack.Marshal(commitMessageBuilder(12, 17))
	require.NoError(t, err)

	resolverCommitCmd := protocol.NewResolverCommit(string(data)).Command(ctx)
	err = rc.Process(ctx, resolverCommitCmd)
	require.NoError(t, err)
	require.NoError(t, resolverCommitCmd.Err())

	require.NoError(t, sq.Shutdown(ctx))
	require.NoError(t, <-errCh)
}

func TestResolver_SSI_Concurrent_Tx(t *testing.T) {
	c := makeResolverConfig(t)

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
	data, err := msgpack.Marshal(commitMessageBuilder(12, 17))
	require.NoError(t, err)

	resolverCommitCmd := protocol.NewResolverCommit(string(data)).Command(ctx)
	err = rc.Process(ctx, resolverCommitCmd)
	require.NoError(t, err)
	require.NoError(t, resolverCommitCmd.Err())

	data, err = msgpack.Marshal(commitMessageBuilder(14, 20))
	require.NoError(t, err)

	resolverCommitCmd = protocol.NewResolverCommit(string(data)).Command(ctx)
	err = rc.Process(ctx, resolverCommitCmd)
	require.ErrorIs(t, protocol.ConvertError(err), ErrTransactionAbort)
	require.ErrorIs(t, protocol.ConvertError(resolverCommitCmd.Err()), ErrTransactionAbort)

	require.NoError(t, sq.Shutdown(ctx))
	require.NoError(t, <-errCh)
}
