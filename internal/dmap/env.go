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

package dmap

import (
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/protocol/resp"
)

// TODO: too dirty. clean that struct
type env struct {
	putCmd         *resp.Put
	putConfig      *putConfig
	hkey           uint64
	timestamp      int64
	flags          int16
	opcode         protocol.OpCode
	replicaOpcode  protocol.OpCode
	command        string
	replicaCommand string
	dmap           string
	key            string
	value          []byte
	timeout        time.Duration
	kind           partitions.Kind
	fragment       *fragment
}
