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

package olricdb

import (
	"context"
	"net/http"
	"net/url"
	"path"
	"time"
)

func (db *OlricDB) findLockKey(hkey uint64, name, key string) (host, error) {
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
		target := url.URL{
			Scheme: db.transport.scheme,
			Host:   owner.String(),
			Path:   path.Join("/find-lock", name, key),
		}
		_, err := db.transport.doRequest(http.MethodGet, target, nil)
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
func (db *OlricDB) waitLockForTimeout(dmp *dmap, key string, timeout time.Duration) {
	defer db.wg.Done()
	unlockCh := dmp.locker.unlockNotifier(key)
	select {
	case <-time.After(timeout):
	case <-db.ctx.Done():
	case <-unlockCh:
		// It's already unlocked
		return
	}
	err := dmp.locker.unlock(key)
	if err == ErrNoSuchLock {
		err = nil
	}
	if err != nil {
		db.logger.Printf("[ERROR] Failed to unlock key: %s", key)
	}
}

func (db *OlricDB) lockKey(hkey uint64, name, key string, timeout time.Duration) error {
	dmp := db.getDMap(name, hkey)
	if dmp.locker.check(key) {
		dmp.locker.lock(key)
		db.wg.Add(1)
		go db.waitLockForTimeout(dmp, key, timeout)
		return nil
	}

	// Find the key or lock among previous owners, if any.
	owner, err := db.findLockKey(hkey, name, key)
	if err != nil {
		return err
	}

	// One of the previous owners has the key, redirect the call.
	if !hostCmp(db.this, owner) {
		target := url.URL{
			Scheme: db.transport.scheme,
			Host:   owner.String(),
			Path:   path.Join("/lock-prev", name, key),
		}
		q := target.Query()
		q.Set("timeout", timeout.String())
		target.RawQuery = q.Encode()
		_, err = db.transport.doRequest(http.MethodGet, target, nil)
		return err
	}

	// This node owns the key/lock. Try to acquire it.
	dmp.locker.lock(key)
	// Wait until the timeout is exceeded and background and release the key if
	// it's still locked.
	db.wg.Add(1)
	go db.waitLockForTimeout(dmp, key, timeout)
	return nil
}

func (db *OlricDB) lockWithTimeout(name, key string, timeout time.Duration) error {
	member, hkey, err := db.locateKey(name, key)
	if err != nil {
		return err
	}
	if !hostCmp(member, db.this) {
		return db.transport.lock(member, name, key, timeout)
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

func (db *OlricDB) unlockKey(hkey uint64, name, key string) error {
	owner, err := db.findLockKey(hkey, name, key)
	if err != nil {
		return err
	}
	if !hostCmp(db.this, owner) {
		target := url.URL{
			Scheme: db.transport.scheme,
			Host:   owner.String(),
			Path:   path.Join("/unlock-prev", name, key),
		}
		_, err = db.transport.doRequest(http.MethodGet, target, nil)
		return err
	}
	dmp := db.getDMap(name, hkey)
	return dmp.locker.unlock(key)
}

func (db *OlricDB) unlock(name, key string) error {
	<-db.bctx.Done()
	if db.bctx.Err() == context.DeadlineExceeded {
		return ErrOperationTimeout
	}

	member, hkey, err := db.locateKey(name, key)
	if err != nil {
		return err
	}
	if !hostCmp(member, db.this) {
		return db.transport.unlock(member, name, key)
	}
	return db.unlockKey(hkey, name, key)
}

// Unlock releases an acquired lock for the given key. It returns ErrNoSuchLock if there is no lock for the given key.
func (dm *DMap) Unlock(key string) error {
	return dm.db.unlock(dm.name, key)
}
