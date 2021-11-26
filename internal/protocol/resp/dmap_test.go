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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProtocol_RESP_GET(t *testing.T) {
	cmd := Get(context.Background(), "mymap", "mykey")
	require.Len(t, cmd.Args(), 3)
	require.Equal(t, fmt.Sprintf("%s mymap mykey: ", GET), cmd.String())
}

func TestProtocol_RESP_PUT(t *testing.T) {
	cmd := Put(context.Background(), "mymap", "mykey", "value")
	require.Len(t, cmd.Args(), 4)
	require.Equal(t, fmt.Sprintf("%s mymap mykey value: ", PUT), cmd.String())
}
