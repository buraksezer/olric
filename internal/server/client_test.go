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

package server

import (
	"context"
	"github.com/buraksezer/olric/internal/protocol/resp"
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestServer_Client_Get(t *testing.T) {
	s := newServer(t)
	defer func() {
		require.NoError(t, s.Shutdown(context.Background()))
	}()

	s.ServeMux().HandleFunc(resp.PingCmd, func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteBulkString("pong")
	})

	<-s.StartedCtx.Done()

	addr := net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort))
	cs := NewClient(defaultRedisOptions(s.config))
	rc := cs.Get(addr)

	ctx := context.Background()
	cmd := resp.NewPing().Command(ctx)
	err := rc.Process(ctx, cmd)
	require.NoError(t, err)

	result, err := cmd.Result()
	require.NoError(t, err)
	require.Equal(t, "pong", result)
}

func TestServer_Client_Close(t *testing.T) {
	s := newServer(t)
	defer func() {
		require.NoError(t, s.Shutdown(context.Background()))
	}()

	<-s.StartedCtx.Done()

	addr := net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort))
	cs := NewClient(defaultRedisOptions(s.config))
	rc1 := cs.Get(addr)

	require.NoError(t, cs.Close(addr))
	rc2 := cs.Get(addr)
	require.NotEqual(t, rc1, rc2)
}
