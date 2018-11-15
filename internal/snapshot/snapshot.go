// Copyright 2018 Burak Sezer
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

package snapshot

import (
	"context"
	"encoding/binary"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/offheap"
	"github.com/dgraph-io/badger"
	"github.com/vmihailenco/msgpack"
)

const (
	defaultSnapshotInterval         = 100 * time.Millisecond
	defaultGCInterval               = 5 * time.Minute
	defaultGCDiscardRatio   float64 = 0.7
)

const (
	opPut uint8 = 0
	opDel uint8 = 1
)

var (
	// PrimaryDMapKey is the key on Badger for registered DMap names on partitions.
	PrimaryDMapKey = []byte("primary-dmap-key")

	// BackupDMapKey is the key on Badger for registered DMap names on backups.
	BackupDMapKey = []byte("backup-dmap-key")
)

type onDiskDMaps map[uint64]map[string]struct{}

// OpLog defines operation log.
type OpLog struct {
	sync.Mutex

	m   map[uint64]uint8
	off *offheap.Offheap
}

// Put logs 'Put' operation for given hkey.
func (o *OpLog) Put(hkey uint64) {
	o.Lock()
	defer o.Unlock()

	o.m[hkey] = opPut
}

// Delete logs 'Delete' operation for given hkey.
func (o *OpLog) Delete(hkey uint64) {
	o.Lock()
	defer o.Unlock()

	o.m[hkey] = opDel
}

func dmapKey(partID uint64, name string) []byte {
	return []byte("dmap-keys-" + name + "-" + strconv.Itoa(int(partID)))
}

// Snapshot implements a low-latency snapshot engine with BadgerDB.
type Snapshot struct {
	mu sync.RWMutex

	db               *badger.DB
	log              *log.Logger
	workers          map[uint64]context.CancelFunc
	oplogs           map[uint64]map[string]*OpLog
	snapshotInterval time.Duration
	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
}

// New creates and returns a new Snapshot.
func New(opt *badger.Options, snapshotInterval, gcInterval time.Duration,
	gcDiscardRatio float64, logger *log.Logger) (*Snapshot, error) {
	if opt == nil {
		opt = &badger.DefaultOptions
	}
	if len(opt.Dir) == 0 {
		dir, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		opt.Dir = dir
	}
	opt.ValueDir = opt.Dir

	if gcDiscardRatio == 0 {
		gcDiscardRatio = defaultGCDiscardRatio
	}

	if gcInterval.Seconds() == 0 {
		gcInterval = defaultGCInterval
	}

	if snapshotInterval.Seconds() == 0 {
		snapshotInterval = defaultSnapshotInterval
	}

	db, err := badger.Open(*opt)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Snapshot{
		db:               db,
		log:              logger,
		workers:          make(map[uint64]context.CancelFunc),
		oplogs:           make(map[uint64]map[string]*OpLog),
		snapshotInterval: snapshotInterval,
		ctx:              ctx,
		cancel:           cancel,
	}
	s.wg.Add(1)
	go s.garbageCollection(gcInterval, gcDiscardRatio)
	return s, nil
}

// Shutdown closes a DB. It's crucial to call it to ensure all the pending updates make their way to disk.
func (s *Snapshot) Shutdown() error {
	select {
	case <-s.ctx.Done():
		return nil
	default:
	}
	s.cancel()
	// Wait for ongoing sync operations.
	s.wg.Wait()
	// Calling DB.Close() multiple times is not safe and would cause panic.
	return s.db.Close()
}

func (s *Snapshot) garbageCollection(gcInterval time.Duration, gcDiscardRatio float64) {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(gcInterval):
		again:
			err := s.db.RunValueLogGC(gcDiscardRatio)
			if err == nil {
				goto again
			}
			s.log.Printf("[DEBUG] BadgerDB GC returned: %v", err)
		}
	}
}

func (s *Snapshot) syncDMap(partID uint64, name string, oplog *OpLog) (map[uint64]uint8, error) {
	oplog.Lock()
	if len(oplog.m) == 0 {
		oplog.Unlock()
		return nil, nil
	}
	// Work on this temporary copy to prevent locking DMap during sync.
	wcopy := make(map[uint64]uint8)
	for hkey, op := range oplog.m {
		wcopy[hkey] = op
		delete(oplog.m, hkey)
	}
	oplog.Unlock()
	s.log.Printf("[DEBUG] DMap: %s on PartID: %d have been synchronizing to BadgerDB", name, partID)

	// Get all the hkeys for this DMap. We need the hkeys slice to restore DMaps at startup.
	var hkeys map[uint64]struct{}
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(dmapKey(partID, name))
		if err == badger.ErrKeyNotFound {
			hkeys = make(map[uint64]struct{})
			return nil
		}
		if err != nil {
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return msgpack.Unmarshal(raw, &hkeys)
	})
	if err != nil {
		return nil, err
	}

	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	// Save failed hkeys to process them again later.
	failed := make(map[uint64]uint8)
	for hkey, op := range wcopy {
		// We need to create a bkey for every hkey because it shouldn't be modified
		// until the underlying transaction is being committed. Otherwise our hkeys
		// in the BadgerDB will be inconsistent.
		bkey := make([]byte, 8)
		binary.BigEndian.PutUint64(bkey, hkey)
		if op == opPut {
			val, err := oplog.off.GetRaw(hkey)
			if err == offheap.ErrKeyNotFound {
				continue
			}
			if err != nil {
				s.log.Printf("[ERROR] Failed to get HKey: %d from in-memory storage: %v", hkey, err)
				failed[hkey] = op
				continue
			}
			err = wb.Set(bkey, val, 0)
			if err != nil {
				s.log.Printf("[ERROR] Failed to set HKey: %d on %s: %v", hkey, name, err)
				failed[hkey] = op
				continue
			}
			// We have the key.
			hkeys[hkey] = struct{}{}
		} else {
			err = wb.Delete(bkey)
			if err != nil {
				s.log.Printf("[ERROR] Failed to delete HKey: %d on %s: %v", hkey, name, err)
				failed[hkey] = op
				continue
			}
			// Delete the hkey from hkeys list.
			delete(hkeys, hkey)
		}
	}
	// Encode available keys to map hkeys to dmaps on Badger.
	data, err := msgpack.Marshal(hkeys)
	if err != nil {
		s.log.Printf("[ERROR] Failed to marshal dmap-keys for %s: %v", name, err)
		return wcopy, wb.Flush()
	}

	err = wb.Set(dmapKey(partID, name), data, 0)
	if err != nil {
		s.log.Printf("[ERROR] Failed to set dmap-keys for %s: %v", name, err)
		// Return the all hkeys to process again. We may lose all of them if this call
		// doesn't work.
		return wcopy, wb.Flush()
	}
	// Failed keys will be processed again by the next call.
	return failed, wb.Flush()
}

// worker runs at background for every partition which has dmaps.
func (s *Snapshot) worker(ctx context.Context, partID uint64) {
	defer s.wg.Done()

	// Iterates over partitions dmaps and calls syncDMap to synchronize DMaps
	// to BadgerDB.
	sync := func() {
		// sync the dmaps to badger.
		s.mu.RLock()
		defer s.mu.RUnlock()

		part, ok := s.oplogs[partID]
		if !ok {
			// There are no registered DMap on this partition.
			return
		}
		for name, oplog := range part {
			failed, err := s.syncDMap(partID, name, oplog)
			if err != nil {
				s.log.Printf("[ERROR] Failed to sync DMap: %s on PartID: %d: %v", name, partID, err)
			}
			// Re-add failed hkeys to OpLog for later processing.
			// It can be pretty critical for our business. We may
			// lose data at that point.
			if len(failed) == 0 {
				continue
			}
			oplog.Lock()
			for hkey, op := range failed {
				_, ok := oplog.m[hkey]
				// If ok is true, the hkey is already updated or deleted by the user.
				// So it will be processed again by the next call.
				if !ok {
					// Add it again to process in the next call.
					oplog.m[hkey] = op
				}
			}
			oplog.Unlock()
		}
	}
	for {
		select {
		case <-s.ctx.Done():
			// Olric instance has been closing. Call sync one last time.
			s.log.Printf("[DEBUG] DMaps on PartID: %d have been synchronizing to BadgerDB for last time.", partID)
			sync()
			return
		case <-ctx.Done():
			// Partition is empty.
			return
		case <-time.After(s.snapshotInterval):
			// Sync DMaps to BadgerDB periodically.
			sync()
		}
	}
}

func (s *Snapshot) registerOnBadger(dkey []byte, partID uint64, name string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		var value onDiskDMaps
		item, err := txn.Get(dkey)
		if err != nil && err != badger.ErrKeyNotFound {
			// Something went wrong.
			return err
		}
		if err == badger.ErrKeyNotFound {
			// Key not found.
			value = make(onDiskDMaps)
		} else {
			// err == nil
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			err = msgpack.Unmarshal(valCopy, &value)
			if err != nil {
				return err
			}
		}
		_, ok := value[partID]
		if !ok {
			value[partID] = make(map[string]struct{})
		}
		value[partID][name] = struct{}{}
		res, err := msgpack.Marshal(value)
		if err != nil {
			return err
		}
		return txn.Set(dkey, res)
	})
}

// RegisterDMap registers given DMap to the snapshot instance, creates a new worker for its partition if needed and returns an OpLog.
func (s *Snapshot) RegisterDMap(dkey []byte, partID uint64, name string, off *offheap.Offheap) (*OpLog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.registerOnBadger(dkey, partID, name)
	if err != nil {
		return nil, err
	}
	if _, ok := s.oplogs[partID]; !ok {
		s.oplogs[partID] = make(map[string]*OpLog)
	}
	oplog := &OpLog{
		m:   make(map[uint64]uint8),
		off: off,
	}
	s.oplogs[partID][name] = oplog

	if _, ok := s.workers[partID]; !ok {
		// Create a worker goroutine for this partition. It will call its sync function peridically
		// to sync dmaps to BadgerDB.
		ctx, cancel := context.WithCancel(context.Background())
		s.workers[partID] = cancel
		s.wg.Add(1)
		go s.worker(ctx, partID)
	}
	return oplog, nil
}

func (s *Snapshot) unregisterOnBadger(dkey []byte, partID uint64, name string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(dkey)
		if err != nil {
			return err
		}

		var value onDiskDMaps
		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		err = msgpack.Unmarshal(valCopy, &value)
		if err != nil {
			return err
		}
		delete(value[partID], name)
		if len(value[partID]) == 0 {
			delete(value, partID)
		}
		res, err := msgpack.Marshal(value)
		if err != nil {
			return err
		}
		return txn.Set(dkey, res)
	})
}

// UnregisterDMap unregisters a dmap from synchronization list.
func (s *Snapshot) UnregisterDMap(dkey []byte, partID uint64, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dmaps, ok := s.oplogs[partID]
	if !ok {
		return nil
	}
	if err := s.unregisterOnBadger(dkey, partID, name); err != nil {
		return err
	}
	delete(dmaps, name)
	if len(dmaps) == 0 {
		delete(s.oplogs, partID)
		// No more data on the partition. Stop the sync worker.
		s.workers[partID]()
	}
	return nil
}

// DestroyDMap destroys a dmap's hkeys and releated data on the snapshot.
func (s *Snapshot) DestroyDMap(dkey []byte, partID uint64, name string) error {
	var hkeys map[uint64]struct{}
	// Retrieve hkeys which belong to dmap from BadgerDB.
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(dmapKey(partID, name))
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return msgpack.Unmarshal(value, &hkeys)
	})
	if err != nil {
		return err
	}

	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	for hkey := range hkeys {
		// bkey is not reusable here. we cannot modify it until the Badger
		// transaction has been committed.
		bkey := make([]byte, 8)
		binary.BigEndian.PutUint64(bkey, hkey)
		err = wb.Delete(bkey)
		if err != nil {
			return err
		}
	}
	err = wb.Flush()
	if err != nil {
		return err
	}
	return s.UnregisterDMap(dkey, partID, name)
}
