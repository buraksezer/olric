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
	"time"

	"github.com/buraksezer/olric/internal/locker"
	"github.com/buraksezer/olric/internal/protocol"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routing_table"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/kvstore"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/buraksezer/olric/serializer"
)

var ErrServerGone = errors.New("server is gone")

var (
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrUnknownOperation means that an unidentified message has been received from a client.
	ErrUnknownOperation = errors.New("unknown operation")
	ErrNotImplemented   = errors.New("not implemented")
	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")
)

var errFragmentNotFound = errors.New("fragment not found")

type storageMap struct {
	engines map[string]storage.Engine
	configs map[string]map[string]interface{}
}

type Service struct {
	sync.RWMutex // protects dmaps map

	log        *flog.Logger
	config     *config.Config
	client     *transport.Client
	rt         *routing_table.RoutingTable
	serializer serializer.Serializer
	primary    *partitions.Partitions
	backup     *partitions.Partitions
	locker     *locker.Locker
	dmaps      map[string]*DMap
	storage    *storageMap
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
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
		locker:     e.Get("locker").(*locker.Locker),
		dmaps:      make(map[string]*DMap),
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

func (dm *DMap) loadFragmentFromPartition(part *partitions.Partition, name string) (*fragment, error) {
	f, ok := part.Map().Load(name)
	if !ok {
		return nil, errFragmentNotFound
	}
	return f.(*fragment), nil
}

func (dm *DMap) createFragmentOnPartition(part *partitions.Partition, name string) (*fragment, error) {
	engine, ok := dm.service.storage.engines[dm.config.storageEngine]
	if !ok {
		return nil, fmt.Errorf("storage engine could not be found: %s", dm.config.storageEngine)
	}
	f := &fragment{}
	var err error
	f.storage, err = engine.Fork(nil)
	if err != nil {
		return nil, err
	}
	part.Map().Store(name, f)
	return f, nil
}

func (dm *DMap) getPartitionByHKey(hkey uint64, kind partitions.Kind) *partitions.Partition {
	var part *partitions.Partition
	if kind == partitions.PRIMARY {
		part = dm.service.primary.PartitionByHKey(hkey)
	} else if kind == partitions.BACKUP {
		part = dm.service.backup.PartitionByHKey(hkey)
	} else {
		// impossible
		panic("unknown partition kind")
	}

	return part
}

func (dm *DMap) getFragment(name string, hkey uint64, kind partitions.Kind) (*fragment, error) {
	part := dm.getPartitionByHKey(hkey, kind)
	part.Lock()
	defer part.Unlock()
	return dm.loadFragmentFromPartition(part, name)
}

func (dm *DMap) getOrCreateFragment(name string, hkey uint64, kind partitions.Kind) (*fragment, error) {
	part := dm.getPartitionByHKey(hkey, kind)
	part.Lock()
	defer part.Unlock()

	// try to get
	f, err := dm.loadFragmentFromPartition(part, name)
	if err == errFragmentNotFound {
		// create the fragment and return
		return dm.createFragmentOnPartition(part, name)
	}
	return f, err
}

func (s *Service) callCompactionOnStorage(f *fragment) {
	defer s.wg.Done()
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()

	for {
		timer.Reset(50 * time.Millisecond)
		select {
		case <-timer.C:
			f.Lock()
			if done := f.storage.Compaction(); done {
				// Fragmented tables are merged. Quit.
				f.Unlock()
				return
			}
			f.Unlock()
		case <-s.ctx.Done():
			return
		}
	}
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

func errorResponse(w protocol.EncodeDecoder, err error) {
	getError := func(err interface{}) []byte {
		switch val := err.(type) {
		case string:
			return []byte(val)
		case error:
			return []byte(val.Error())
		default:
			return nil
		}
	}
	w.SetValue(getError(err))

	switch {
	case err == ErrOperationTimeout, errors.Is(err, ErrOperationTimeout):
		w.SetStatus(protocol.StatusErrOperationTimeout)
	case err == routing_table.ErrClusterQuorum, errors.Is(err, routing_table.ErrClusterQuorum):
		w.SetStatus(protocol.StatusErrClusterQuorum)
	case err == ErrUnknownOperation, errors.Is(err, ErrUnknownOperation):
		w.SetStatus(protocol.StatusErrUnknownOperation)
	case err == ErrServerGone, errors.Is(err, ErrServerGone):
		w.SetStatus(protocol.StatusErrServerGone)
	case err == ErrInvalidArgument, errors.Is(err, ErrInvalidArgument):
		w.SetStatus(protocol.StatusErrInvalidArgument)
	case err == ErrNotImplemented, errors.Is(err, ErrNotImplemented):
		w.SetStatus(protocol.StatusErrNotImplemented)
	case err == ErrKeyNotFound || err == storage.ErrKeyNotFound || err == errFragmentNotFound:
		w.SetStatus(protocol.StatusErrKeyNotFound)
	default:
		w.SetStatus(protocol.StatusInternalServerError)
	}
}
