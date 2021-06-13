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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/buraksezer/olric/pkg/storage"
)

// pool is good for recycling memory while reading messages from the socket.
var bufferPool = bufpool.New()

const nilTimeout = 0 * time.Second

var (
	// ErrKeyNotFound is returned when a key could not be found.
	ErrKeyNotFound  = neterrors.New(protocol.StatusErrKeyNotFound, "key not found")
	ErrDMapNotFound = errors.New("dmap not found")
)

// DMap implements a single hop distributed hash table.
type DMap struct {
	name   string
	s      *Service
	engine storage.Engine
	config *dmapConfig
}

// getDMap returns an initialized DMap instance, otherwise it returns ErrDMapNotFound.
func (s *Service) getDMap(name string) (*DMap, error) {
	s.RLock()
	defer s.RUnlock()

	dm, ok := s.dmaps[name]
	if !ok {
		return nil, ErrDMapNotFound
	}
	return dm, nil
}

// NewDMap creates and returns a new DMap instance. It checks member count quorum and bootstrapping status before creating a new DMap.
func (s *Service) NewDMap(name string) (*DMap, error) {
	// Check operation status first:
	//
	// * Checks member count in the cluster, returns ErrClusterQuorum if
	//   the quorum value cannot be satisfied,
	// * Checks bootstrapping status and awaits for a short period before
	//   returning ErrRequest timeout.
	if err := s.rt.CheckMemberCountQuorum(); err != nil {
		return nil, err
	}
	// An Olric node has to be bootstrapped to function properly.
	if err := s.rt.CheckBootstrap(); err != nil {
		return nil, err
	}

	s.Lock()
	defer s.Unlock()

	dm, ok := s.dmaps[name]
	if ok {
		return dm, nil
	}

	dm = &DMap{
		config: &dmapConfig{},
		name:   name,
		s:      s,
	}
	if err := dm.config.load(s.config.DMaps, name); err != nil {
		return nil, err
	}

	engine, ok := dm.s.storage.engines[dm.config.storageEngine]
	if !ok {
		return nil, fmt.Errorf("storage engine could not be found: %s", dm.config.storageEngine)
	}
	dm.engine = engine

	s.dmaps[name] = dm
	return dm, nil
}

// getOrCreate is a shortcut function to create a new DMap or get an already initialized DMap instance.
func (s *Service) getOrCreateDMap(name string) (*DMap, error) {
	dm, err := s.getDMap(name)
	if errors.Is(err, ErrDMapNotFound) {
		return s.NewDMap(name)
	}
	return dm, err
}

func (dm *DMap) loadFragmentFromPartition(part *partitions.Partition) (*fragment, error) {
	f, ok := part.Map().Load(dm.name)
	if !ok {
		return nil, errFragmentNotFound
	}
	return f.(*fragment), nil
}

func (dm *DMap) createFragmentOnPartition(part *partitions.Partition) (*fragment, error) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fragment{
		service:   dm.s,
		accessLog: newAccessLog(),
		ctx:       ctx,
		cancel:    cancel,
	}
	var err error
	f.storage, err = dm.engine.Fork(nil)
	if err != nil {
		return nil, err
	}
	part.Map().Store(dm.name, f)
	return f, nil
}

func (dm *DMap) getPartitionByHKey(hkey uint64, kind partitions.Kind) *partitions.Partition {
	var part *partitions.Partition
	switch {
	case kind == partitions.PRIMARY:
		part = dm.s.primary.PartitionByHKey(hkey)
	case kind == partitions.BACKUP:
		part = dm.s.backup.PartitionByHKey(hkey)
	default:
		panic("unknown partition kind")
	}
	return part
}

func (dm *DMap) getFragment(hkey uint64, kind partitions.Kind) (*fragment, error) {
	part := dm.getPartitionByHKey(hkey, kind)
	part.Lock()
	defer part.Unlock()
	return dm.loadFragmentFromPartition(part)
}

func (dm *DMap) getOrCreateFragment(hkey uint64, kind partitions.Kind) (*fragment, error) {
	part := dm.getPartitionByHKey(hkey, kind)
	part.Lock()
	defer part.Unlock()

	// try to get
	f, err := dm.loadFragmentFromPartition(part)
	if errors.Is(err, errFragmentNotFound) {
		// create the fragment and return
		return dm.createFragmentOnPartition(part)
	}
	return f, err
}

func timeoutToTTL(timeout time.Duration) int64 {
	if timeout.Seconds() == 0 {
		return 0
	}
	// convert nanoseconds to milliseconds
	return (timeout.Nanoseconds() + time.Now().UnixNano()) / 1000000
}

func isKeyExpired(ttl int64) bool {
	if ttl == 0 {
		return false
	}

	// convert nanoseconds to milliseconds
	res := (time.Now().UnixNano() / 1000000) >= ttl
	if res {
		// number of valid items removed from cache to free memory for new items.
		EvictedTotal.Increase(1)
	}
	return res
}
