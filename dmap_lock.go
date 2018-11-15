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

package olric

import (
	"context"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

func (db *Olric) findLockKey(hkey uint64, name, key string) (host, error) {
	part := db.getPartition(hkey)
	part.RLock()
	defer part.RUnlock()
	if len(part.owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	if len(part.owners) == 1 {
		if hostCmp(db.this, part.owners[0]) {
			return db.this, nil
		}
	}
	for i := 1; i <= len(part.owners); i++ {
		// Traverse in reverse order.
		idx := len(part.owners) - i
		owner := part.owners[idx]
		if hostCmp(db.this, owner) {
			continue
		}
		req := &protocol.Message{
			DMap: name,
			Key:  key,
		}
		_, err := db.requestTo(owner.String(), protocol.OpFindLock, req)
		if err == nil {
			return owner, nil
		}
		if err == ErrNoSuchLock {
			err = nil
		}
		if err != nil {
			return host{}, err
		}
	}
	return db.this, nil
}

// Wait until the timeout is exceeded and background and release the key if it's still locked.
func (db *Olric) waitLockForTimeout(dm *dmap, key string, timeout time.Duration) {
	defer db.wg.Done()
	unlockCh := dm.locker.unlockNotifier(key)
	select {
	case <-time.After(timeout):
	case <-db.ctx.Done():
	case <-unlockCh:
		// It's already unlocked
		return
	}
	err := dm.locker.unlock(key)
	if err == ErrNoSuchLock {
		err = nil
	}
	if err != nil {
		db.log.Printf("[ERROR] Failed to unlock key: %s", key)
	}
}

func (db *Olric) lockKey(hkey uint64, name, key string, timeout time.Duration) error {
	dm, err := db.getDMap(name, hkey)
	if err != nil {
		return err
	}
	if dm.locker.check(key) {
		dm.locker.lock(key)
		db.wg.Add(1)
		go db.waitLockForTimeout(dm, key, timeout)
		return nil
	}

	// Find the key or lock among previous owners, if any.
	owner, err := db.findLockKey(hkey, name, key)
	if err != nil {
		return err
	}

	// One of the previous owners has the key, redirect the call.
	if !hostCmp(db.this, owner) {
		req := &protocol.Message{
			DMap:  name,
			Key:   key,
			Extra: protocol.LockWithTimeoutExtra{TTL: timeout.Nanoseconds()},
		}
		_, err := db.requestTo(owner.String(), protocol.OpLockPrev, req)
		return err
	}

	// This node owns the key/lock. Try to acquire it.
	dm.locker.lock(key)
	// Wait until the timeout is exceeded and background and release the key if
	// it's still locked.
	db.wg.Add(1)
	go db.waitLockForTimeout(dm, key, timeout)
	return nil
}

func (db *Olric) lockWithTimeout(name, key string, timeout time.Duration) error {
	member, hkey, err := db.locateKey(name, key)
	if err != nil {
		return err
	}
	if !hostCmp(member, db.this) {
		req := &protocol.Message{
			DMap:  name,
			Key:   key,
			Extra: protocol.LockWithTimeoutExtra{TTL: timeout.Nanoseconds()},
		}
		_, err = db.requestTo(member.String(), protocol.OpExLockWithTimeout, req)
		return err
	}
	return db.lockKey(hkey, name, key, timeout)
}

// LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time,
// it automatically releases the lock. Acquired lock is only for the key in this map.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until timeout.
// The timeout is determined by http.Client which can be configured via Config structure.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (dm *DMap) LockWithTimeout(key string, timeout time.Duration) error {
	return dm.db.lockWithTimeout(dm.name, key, timeout)
}

func (db *Olric) unlockKey(hkey uint64, name, key string) error {
	owner, err := db.findLockKey(hkey, name, key)
	if err != nil {
		return err
	}
	if !hostCmp(db.this, owner) {
		req := &protocol.Message{
			DMap: name,
			Key:  key,
		}
		_, err = db.requestTo(owner.String(), protocol.OpUnlockPrev, req)
		return err
	}
	dm, err := db.getDMap(name, hkey)
	if err != nil {
		return err
	}
	return dm.locker.unlock(key)
}

func (db *Olric) unlock(name, key string) error {
	<-db.bcx.Done()
	if db.bcx.Err() == context.DeadlineExceeded {
		return ErrOperationTimeout
	}

	member, hkey, err := db.locateKey(name, key)
	if err != nil {
		return err
	}
	if !hostCmp(member, db.this) {
		req := &protocol.Message{
			DMap: name,
			Key:  key,
		}
		_, err = db.requestTo(member.String(), protocol.OpExUnlock, req)
		return err
	}
	return db.unlockKey(hkey, name, key)
}

// Unlock releases an acquired lock for the given key. It returns ErrNoSuchLock if there is no lock for the given key.
func (dm *DMap) Unlock(key string) error {
	return dm.db.unlock(dm.name, key)
}

func (db *Olric) exLockWithTimeoutOperation(req *protocol.Message) *protocol.Message {
	ttl := req.Extra.(protocol.LockWithTimeoutExtra).TTL
	err := db.lockWithTimeout(req.DMap, req.Key, time.Duration(ttl))
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	return req.Success()
}

func (db *Olric) exUnlockOperation(req *protocol.Message) *protocol.Message {
	err := db.unlock(req.DMap, req.Key)
	if err == ErrNoSuchLock {
		return req.Error(protocol.StatusNoSuchLock, "")
	}
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	return req.Success()
}

func (db *Olric) findLockOperation(req *protocol.Message) *protocol.Message {
	hkey := db.getHKey(req.DMap, req.Key)
	dm, err := db.getDMap(req.DMap, hkey)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	if dm.locker.check(req.Key) {
		return req.Success()
	}
	return req.Error(protocol.StatusNoSuchLock, "")
}

func (db *Olric) unlockPrevOperation(req *protocol.Message) *protocol.Message {
	key := req.Key
	hkey := db.getHKey(req.DMap, key)
	dm, err := db.getDMap(req.DMap, hkey)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	err = dm.locker.unlock(key)
	if err == ErrNoSuchLock {
		return req.Error(protocol.StatusNoSuchLock, "")
	}
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	return req.Success()
}

func (db *Olric) lockPrevOperation(req *protocol.Message) *protocol.Message {
	key := req.Key
	hkey := db.getHKey(req.DMap, key)
	dm, err := db.getDMap(req.DMap, hkey)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	dm.locker.lock(key)
	db.wg.Add(1)
	ttl := req.Extra.(protocol.LockWithTimeoutExtra).TTL
	go db.waitLockForTimeout(dm, key, time.Duration(ttl))
	return req.Success()
}
