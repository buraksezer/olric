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

package zmap

import (
	"testing"

	"github.com/buraksezer/olric/internal/testzmap"
	"github.com/stretchr/testify/require"
)

func TestZMap_TX(t *testing.T) {
	tz := testzmap.New(t, NewService)
	s := tz.AddStorageNode(nil).(*Service)

	zc := testZMapConfig(t)
	zm, err := s.NewZMap("myzmap", zc)
	require.NoError(t, err)

	_, err = zm.Tx()
	require.NoError(t, err)
}

func TestZMap_getReadVersion(t *testing.T) {
	tz := testzmap.New(t, NewService)
	s := tz.AddStorageNode(nil).(*Service)

	readVersion, err := s.getReadVersion()
	require.NoError(t, err)
	require.Equal(t, uint32(0), readVersion)

	zc := testZMapConfig(t)
	zm, err := s.NewZMap("myzmap", zc)
	require.NoError(t, err)

	_, err = zm.Tx()
	require.NoError(t, err)
}
