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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/buraksezer/olric/internal/kvstore"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routing_table"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/buraksezer/olric/serializer"
)

type storageMap struct {
	engines map[string]storage.Engine
	configs map[string]map[string]interface{}
}

type Service struct {
	sync.RWMutex

	log        *flog.Logger
	config     *config.Config
	client     *transport.Client
	rt         *routing_table.RoutingTable
	serializer serializer.Serializer
	primary    *partitions.Partitions
	backup     *partitions.Partitions
	// Map of storage engines
	storage *storageMap
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewService(e *environment.Environment) (*Service, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		config:     e.Get("config").(*config.Config),
		serializer: e.Get("config").(*config.Config).Serializer,
		client:     e.Get("client").(*transport.Client),
		log:        e.Get("logger").(*flog.Logger),
		rt:         e.Get("routingTable").(*routing_table.RoutingTable),
		primary:    e.Get("primary").(*partitions.Partitions),
		backup:     e.Get("backup").(*partitions.Partitions),
		ctx:        ctx,
		cancel:     cancel,
	}
	err := s.initializeAndLoadStorageEngines()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) initializeAndLoadStorageEngines() error {
	s.storage.configs = s.config.StorageEngines.Config
	s.storage.engines = s.config.StorageEngines.Impls

	// Load engines as plugin, if any.
	for _, pluginPath := range s.config.StorageEngines.Plugins {
		engine, err := storage.LoadAsPlugin(pluginPath)
		if err != nil {
			return err
		}
		s.storage.engines[engine.Name()] = engine
	}

	// Set a default engine, if required.
	if len(s.config.StorageEngines.Impls) == 0 {
		if _, ok := s.config.StorageEngines.Config[config.DefaultStorageEngine]; !ok {
			return errors.New("no storage engine defined")
		}
		s.storage.engines[config.DefaultStorageEngine] = &kvstore.KVStore{}
	}

	// Set configuration for the loaded engines.
	for name, ec := range s.config.StorageEngines.Config {
		engine, ok := s.storage.engines[name]
		if !ok {
			return fmt.Errorf("storage engine implementation is missing: %s", name)
		}
		engine.SetConfig(storage.NewConfig(ec))
	}

	// Start the engines.
	for _, engine := range s.storage.engines {
		engine.SetLogger(s.config.Logger)
		if err := engine.Start(); err != nil {
			return err
		}
		s.log.V(2).Printf("[INFO] Storage engine has been loaded: %s", engine.Name())
	}
	return nil
}

var errFragmentNotFound = errors.New("fragment not found")

func (dm *DMap) loadFragment(part *partitions.Partition, name string) (*fragment, error) {
	f, ok := part.Map().Load(name)
	if !ok {
		return nil, errFragmentNotFound
	}
	return f.(*fragment), nil
}

func (dm *DMap) createFragment(part *partitions.Partition, name string) (*fragment, error){
	engine, ok := dm.service.storage.engines[dm.config.storageEngine]
	if !ok {
		return nil, fmt.Errorf("storage engine could not be found: %s", dm.config.storageEngine)
	}
	fg := &fragment{}
	var err error
	fg.storage, err = engine.Fork(nil)
	if err != nil {
		return nil, err
	}
	part.Map().Store(name, fg)
	return fg, nil
}

func (dm *DMap) getFragmentByHKey(name string, hkey uint64, kind partitions.Kind) (*fragment, error) {
	var part *partitions.Partition
	if kind == partitions.PRIMARY {
		part = dm.service.primary.PartitionByHKey(hkey)
	} else if kind == partitions.BACKUP {
		part = dm.service.backup.PartitionByHKey(hkey)
	} else {
		return nil, errors.New("unknown partition kind")
	}

	part.Lock()
	defer part.Unlock()

	// try to get
	f, err := dm.loadFragment(part, name)
	if err == errFragmentNotFound {
		// create the fragment and return
		return dm.createFragment(part, name)
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.cancel()
	done := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return err
		}
	case <-done:
	}
	return nil
}
