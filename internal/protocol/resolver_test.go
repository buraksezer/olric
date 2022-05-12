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

package protocol

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProtocol_ResolverCommit(t *testing.T) {
	body := "{\"foo\":\"bar\"}"
	rqCmd := NewResolverCommit(body)

	cmd := stringToCommand(rqCmd.Command(context.Background()).String())
	commitCmd, err := ParseResolverCommit(cmd)
	require.NoError(t, err)
	require.Equal(t, body, commitCmd.Body)

	t.Run("RESOLVER.COMMIT invalid command", func(t *testing.T) {
		cmd := stringToCommand("RESOLVER.COMMIT foo bar")
		_, err := ParseResolverCommit(cmd)
		require.Error(t, err)
	})
}
