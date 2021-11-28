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

package resp

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func stringToCommand(s string) redcon.Command {
	cmd := redcon.Command{
		Raw: []byte(s),
	}

	s = strings.TrimSuffix(s, ": ")
	parsed := strings.Split(s, " ")
	for _, arg := range parsed {
		cmd.Args = append(cmd.Args, []byte(arg))
	}
	return cmd
}

func TestProtocol_ParsePutCommand(t *testing.T) {
	putCmd := Put(context.Background(), "my-dmap", "my-key", "my-value")
	cmd := stringToCommand(putCmd.String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
}

func TestProtocol_ParseGetCommand(t *testing.T) {
	getCmd := Get(context.Background(), "my-dmap", "my-key")
	cmd := stringToCommand(getCmd.String())
	parsed, err := ParseGetCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
}
