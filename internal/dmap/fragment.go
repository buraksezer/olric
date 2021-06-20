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
	"sync"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/vmihailenco/msgpack"
)

type fragment struct {
	sync.RWMutex

	service   *Service
	storage   storage.Engine
	accessLog *accessLog
	ctx       context.Context
	cancel    context.CancelFunc
}

func (f *fragment) Stats() storage.Stats {
	return f.storage.Stats()
}

func (f *fragment) Compaction() (bool, error) {
	select {
	case <-f.ctx.Done():
		// fragment is closed or destroyed
		return false, nil
	default:
	}
	return f.storage.Compaction()
}

func (f *fragment) Destroy() error {
	select {
	case <-f.ctx.Done():
		return f.storage.Destroy()
	default:
	}
	return errors.New("fragment is not closed")
}

func (f *fragment) Close() error {
	defer f.cancel()
	return f.storage.Close()
}

func (f *fragment) Name() string {
	return "DMap"
}

func (f *fragment) Length() int {
	f.RLock()
	defer f.RUnlock()

	return f.storage.Stats().Length
}

func (f *fragment) Move(partID uint64, kind partitions.Kind, name string, owner discovery.Member) error {
	f.Lock()
	defer f.Unlock()

	payload, err := f.storage.Export()
	if err != nil {
		return err
	}
	fp := &fragmentPack{
		PartID:    partID,
		Kind:      kind,
		Name:      name,
		Payload:   payload,
		AccessLog: f.accessLog.m,
	}
	value, err := msgpack.Marshal(fp)
	if err != nil {
		return err
	}

	req := protocol.NewSystemMessage(protocol.OpMoveFragment)
	req.SetValue(value)
	_, err = f.service.requestTo(owner.String(), req)
	return err
}

func (dm *DMap) newFragment() (*fragment, error) {
	str, err := dm.engine.Fork(nil)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &fragment{
		service:   dm.s,
		accessLog: newAccessLog(),
		storage:   str,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func (dm *DMap) loadOrCreateFragment(part *partitions.Partition) (*fragment, error) {
	part.Lock()
	defer part.Unlock()

	// Creating a new fragment is our critical section here.
	// It should be protected by a lock.

	fg, ok := part.Map().Load(dm.name)
	if ok {
		return fg.(*fragment), nil
	}

	f, err := dm.newFragment()
	if err != nil {
		return nil, err
	}

	part.Map().Store(dm.name, f)
	return f, nil
}

func (dm *DMap) loadFragment(part *partitions.Partition) (*fragment, error) {
	f, ok := part.Map().Load(dm.name)
	if !ok {
		return nil, errFragmentNotFound
	}
	return f.(*fragment), nil
}

var _ partitions.Fragment = (*fragment)(nil)
