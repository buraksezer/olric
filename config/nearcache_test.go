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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_NearCache(t *testing.T) {
	nc := NearCache{}

	require.NoError(t, nc.Sanitize())
	require.NoError(t, nc.Validate())

	require.Greater(t, nc.MaxInuse, -1)
	require.Greater(t, nc.MaxKeys, -1)
	require.Equal(t, DefaultLRUSamples, nc.LRUSamples)
}
