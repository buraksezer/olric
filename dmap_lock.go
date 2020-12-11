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

package olric

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

var (
	// ErrLockNotAcquired is returned when the requested lock could not be acquired
	ErrLockNotAcquired = errors.New("lock not acquired")

	// ErrNoSuchLock is returned when the requested lock does not exist
	ErrNoSuchLock = errors.New("no such lock")
)

// LockContext is returned by Lock and LockWithTimeout methods.
// It should be stored in a proper way to release the lock.
type LockContext struct {
	name  string
	key   string
	token []byte
	db    *Olric
}

// unlockKey tries to unlock the lock by verifying the lock with token.
func (db *Olric) unlockKey(name, key string, token []byte) error {
	lkey := name + key
	// Only one unlockKey should work for a given key.
	db.locker.Lock(lkey)
	defer func() {
		err := db.locker.Unlock(lkey)
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", key, name, err)
		}
	}()

	// get the key to check its value
	entry, err := db.get(name, key)
	if err == ErrKeyNotFound {
		return ErrNoSuchLock
	}
	if err != nil {
		return err
	}
	val, err := db.unmarshalValue(entry.Value)
	if err != nil {
		return err
	}

	// the locks is released by the node(timeout) or the user
	if !bytes.Equal(val.([]byte), token) {
		return ErrNoSuchLock
	}

	// release it.
	err = db.deleteKey(name, key)
	if err != nil {
		return fmt.Errorf("unlock failed because of delete: %w", err)
	}
	return nil
}

// unlock takes key and token and tries to unlock the key.
// It redirects the request to the partition owner, if required.
func (db *Olric) unlock(name, key string, token []byte) error {
	member, _ := db.findPartitionOwner(name, key)
	if cmpMembersByName(member, db.this) {
		return db.unlockKey(name, key, token)
	}
	req := protocol.NewDMapMessage(protocol.OpUnlock)
	req.SetDMap(name)
	req.SetKey(key)
	req.SetValue(token)
	_, err := db.requestTo(member.String(), req)
	return err
}

// Unlock releases the lock.
func (l *LockContext) Unlock() error {
	return l.db.unlock(l.name, l.key, l.token)
}

// tryLock takes a deadline and writeop and sets a key-value pair by using
// PutIf or PutIfEx commands. It tries to acquire the lock 100 times per second
// if the lock is already acquired. It returns ErrLockNotAcquired if the deadline exceeds.
func (db *Olric) tryLock(w *writeop, deadline time.Duration) error {
	err := db.put(w)
	if err == nil {
		return nil
	}
	// If it returns ErrKeyFound, the lock is already acquired.
	if err != ErrKeyFound {
		// something went wrong
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()

	// Try to acquire lock.
LOOP:
	for {
		timer.Reset(10 * time.Millisecond)
		select {
		case <-timer.C:
			err = db.put(w)
			if err == ErrKeyFound {
				// not released by the other process/goroutine. try again.
				continue
			}
			if err != nil {
				// something went wrong.
				return err
			}
			// Acquired! Quit without error.
			break LOOP
		case <-ctx.Done():
			// Deadline exceeded. Quit with an error.
			return ErrLockNotAcquired
		case <-db.ctx.Done():
			return fmt.Errorf("server is gone")
		}
	}
	return nil
}

// lockKey prepares a token and writeop calls tryLock
func (db *Olric) lockKey(opcode protocol.OpCode, name, key string,
	timeout, deadline time.Duration) (*LockContext, error) {
	token := make([]byte, 16)
	_, err := rand.Read(token)
	if err != nil {
		return nil, err
	}
	w, err := db.prepareWriteop(opcode, name, key, token, timeout, IfNotFound)
	if err != nil {
		return nil, err
	}
	err = db.tryLock(w, deadline)
	if err != nil {
		return nil, err
	}
	return &LockContext{
		name:  name,
		key:   key,
		token: token,
		db:    db,
	}, nil
}

// LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time,
// it automatically releases the lock. Acquired lock is only for the key in this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (dm *DMap) LockWithTimeout(key string, timeout, deadline time.Duration) (*LockContext, error) {
	return dm.db.lockKey(protocol.OpPutIfEx, dm.name, key, timeout, deadline)
}

// Lock sets a lock for the given key. Acquired lock is only for the key in this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (dm *DMap) Lock(key string, deadline time.Duration) (*LockContext, error) {
	return dm.db.lockKey(protocol.OpPutIf, dm.name, key, nilTimeout, deadline)
}

func (db *Olric) exLockWithTimeoutOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	timeout := req.Extra().(protocol.LockWithTimeoutExtra).Timeout
	deadline := req.Extra().(protocol.LockWithTimeoutExtra).Deadline
	ctx, err := db.lockKey(protocol.OpPutIfEx, req.DMap(), req.Key(), time.Duration(timeout), time.Duration(deadline))
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(ctx.token)
}

func (db *Olric) exLockOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	deadline := req.Extra().(protocol.LockExtra).Deadline
	ctx, err := db.lockKey(protocol.OpPutIf, req.DMap(), req.Key(), nilTimeout, time.Duration(deadline))
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(ctx.token)
}

func (db *Olric) exUnlockOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	err := db.unlock(req.DMap(), req.Key(), req.Value())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
