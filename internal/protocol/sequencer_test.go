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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProtocol_SequencerGetCommitVersion(t *testing.T) {
	sqCmd := NewSequencerGetCommitVersion()

	cmd := stringToCommand(sqCmd.Command(context.Background()).String())
	_, err := ParseSequencerGetCommitVersion(cmd)
	require.NoError(t, err)

	t.Run("SEQUENCER.GETCOMMITVERSION invalid command", func(t *testing.T) {
		cmd := stringToCommand("sequencer.getcommitversion foo bar")
		_, err = ParseSequencerGetCommitVersion(cmd)
		require.Error(t, err)
	})
}

func TestProtocol_SequencerGetReadVersion(t *testing.T) {
	sqCmd := NewSequencerGetReadVersion()

	cmd := stringToCommand(sqCmd.Command(context.Background()).String())
	_, err := ParseSequencerGetReadVersion(cmd)
	require.NoError(t, err)

	t.Run("SEQUENCER.GETREADVERSION invalid command", func(t *testing.T) {
		cmd := stringToCommand("sequencer.getreadversion foo bar")
		_, err = ParseSequencerGetReadVersion(cmd)
		require.Error(t, err)
	})
}

func TestProtocol_SequencerUpdateReadVersion(t *testing.T) {
	sqCmd := NewSequencerUpdateReadVersion(1231)

	cmd := stringToCommand(sqCmd.Command(context.Background()).String())
	res, err := ParseSequencerUpdateReadVersion(cmd)
	require.NoError(t, err)
	require.Equal(t, int64(1231), res.CommitVersion)

	t.Run("SEQUENCER.UPDATEREADVERSION invalid command", func(t *testing.T) {
		cmd := stringToCommand("sequencer.updatereadversion foo bar")
		_, err = ParseSequencerUpdateReadVersion(cmd)
		require.Error(t, err)
	})
}
