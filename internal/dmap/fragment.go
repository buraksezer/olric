// Copyright 2018-2020 Burak Sezer
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
	"sync"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/pkg/storage"
)

type fragment struct {
	sync.RWMutex

	service *Service
	storage storage.Engine
}

func (f *fragment) Name() string {
	return "DMap"
}

func (f *fragment) Length() int {
	f.RLock()
	defer f.RUnlock()

	return f.storage.Stats().Length
}

func (f *fragment) Move(_ uint64, _ partitions.Kind, _ string, _ discovery.Member) error {
	return nil
}

var _ partitions.Fragment = (*fragment)(nil)
