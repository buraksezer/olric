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
	"encoding/json"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	_, b, _, _ = runtime.Caller(0)
	basepath   = filepath.Dir(b)
)

func TestConfig_CompareJSON(t *testing.T) {
	filename := path.Join(basepath, "fixtures/olric-transaction-log.yaml")
	c, err := New(filename)
	require.NoError(t, err)

	data, err := json.MarshalIndent(c, "", "  ")
	require.NoError(t, err)

	jsonFilename := path.Join(basepath, "fixtures/olric-transaction-log.json")
	f, err := os.Open(jsonFilename)
	require.NoError(t, err)
	jsonData, err := io.ReadAll(f)
	require.NoError(t, err)

	require.Equal(t, string(jsonData), string(data))
}
