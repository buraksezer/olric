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

func TestProtocol_SequencerCommitVersion(t *testing.T) {
	sqCmd := NewSequencerCommitVersion()

	cmd := stringToCommand(sqCmd.Command(context.Background()).String())
	_, err := ParseSequencerCommitVersion(cmd)
	require.NoError(t, err)

	t.Run("SEQUENCER.COMMITVERSION invalid command", func(t *testing.T) {
		cmd := stringToCommand("sequencer.commitversion foo bar")
		_, err = ParseSequencerCommitVersion(cmd)
		require.Error(t, err)
	})
}

func TestProtocol_SequencerReadVersion(t *testing.T) {
	sqCmd := NewSequencerReadVersion()

	cmd := stringToCommand(sqCmd.Command(context.Background()).String())
	_, err := ParseSequencerReadVersion(cmd)
	require.NoError(t, err)

	t.Run("SEQUENCER.READVERSION invalid command", func(t *testing.T) {
		cmd := stringToCommand("sequencer.readversion foo bar")
		_, err = ParseSequencerReadVersion(cmd)
		require.Error(t, err)
	})
}
