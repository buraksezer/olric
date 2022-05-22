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

func TestProtocol_TransactionLogAdd(t *testing.T) {
	data := []byte("foobar")
	tsCmd := NewTransactionLogAdd(10, data)

	cmd := stringToCommand(tsCmd.Command(context.Background()).String())
	ts, err := ParseTransactionLogAdd(cmd)
	require.NoError(t, err)
	require.Equal(t, uint32(10), ts.CommitVersion)
	require.Equal(t, data, ts.Data)

	t.Run("TransactionLog.Add invalid command", func(t *testing.T) {
		cmd := stringToCommand("TransactionLog.Add foo bar")
		_, err = ParseTransactionLogAdd(cmd)
		require.Error(t, err)
	})
}

func TestProtocol_TransactionLogGet(t *testing.T) {
	tsCmd := NewTransactionLogGet(20)

	cmd := stringToCommand(tsCmd.Command(context.Background()).String())
	ts, err := ParseTransactionLogGet(cmd)
	require.NoError(t, err)
	require.Equal(t, uint32(20), ts.ReadVersion)

	t.Run("TransactionLog.Get invalid command", func(t *testing.T) {
		cmd := stringToCommand("TransactionLog.Get foo bar")
		_, err = ParseTransactionLogGet(cmd)
		require.Error(t, err)
	})
}
