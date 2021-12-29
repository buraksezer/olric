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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
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
	key   string
	token []byte
	dm    *DMap
}

// unlockKey tries to unlock the lock by verifying the lock with token.
func (dm *DMap) unlockKey(key string, token []byte) error {
	lkey := dm.name + key
	// Only one unlockKey should work for a given key.
	dm.s.locker.Lock(lkey)
	defer func() {
		err := dm.s.locker.Unlock(lkey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", key, dm.name, err)
		}
	}()

	// get the key to check its value
	entry, err := dm.get(key)
	if errors.Is(err, ErrKeyNotFound) {
		return ErrNoSuchLock
	}
	if err != nil {
		return err
	}

	// the lock is released by the node(timeout) or the user
	if !bytes.Equal(entry.Value(), token) {
		return ErrNoSuchLock
	}

	// release it.
	err = dm.deleteKey(key)
	if err != nil {
		return fmt.Errorf("unlock failed because of delete: %w", err)
	}
	return nil
}

// unlock takes key and token and tries to unlock the key.
// It redirects the request to the partition owner, if required.
func (dm *DMap) unlock(key string, token []byte) error {
	hkey := partitions.HKey(dm.name, key)
	member := dm.s.primary.PartitionByHKey(hkey).Owner()
	if member.CompareByName(dm.s.rt.This()) {
		return dm.unlockKey(key, token)
	}

	cmd := protocol.NewUnlock(dm.name, key, hex.EncodeToString(token)).Command(dm.s.ctx)
	rc := dm.s.respClient.Get(member.String())
	err := rc.Process(dm.s.ctx, cmd)
	if err != nil {
		return protocol.ConvertError(err)
	}
	return protocol.ConvertError(cmd.Err())
}

// Unlock releases the lock.
func (l *LockContext) Unlock() error {
	return l.dm.unlock(l.key, l.token)
}

// tryLock takes a deadline and env and sets a key-value pair by using
// PutIf or PutIfEx commands. It tries to acquire the lock 100 times per second
// if the lock is already acquired. It returns ErrLockNotAcquired if the deadline exceeds.
func (dm *DMap) tryLock(e *env, deadline time.Duration) error {
	err := dm.put(e)
	if err == nil {
		return nil
	}
	// If it returns ErrKeyFound, the lock is already acquired.
	if !errors.Is(err, ErrKeyFound) {
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
			err = dm.put(e)
			if errors.Is(err, ErrKeyFound) {
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
		case <-dm.s.ctx.Done():
			return fmt.Errorf("server is gone")
		}
	}
	return nil
}

// lockKey prepares a token and env, then calls tryLock
func (dm *DMap) lockKey(key string, timeout, deadline time.Duration) (*LockContext, error) {
	token := make([]byte, 16)
	_, err := rand.Read(token)
	if err != nil {
		return nil, err
	}

	var options []PutOption
	options = append(options, NX())
	if timeout.Milliseconds() != 0 {
		options = append(options, PX(timeout))
	}

	var pc putConfig
	for _, opt := range options {
		opt(&pc)
	}

	e := newEnv()
	e.putConfig = &pc
	e.dmap = dm.name
	e.key = key
	e.value = token
	err = dm.tryLock(e, deadline)
	if err != nil {
		return nil, err
	}
	return &LockContext{
		key:   key,
		token: token,
		dm:    dm,
	}, nil
}

// LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time,
// it automatically releases the lock. Acquired lock is only for the key in this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (dm *DMap) LockWithTimeout(key string, timeout, deadline time.Duration) (*LockContext, error) {
	return dm.lockKey(key, timeout, deadline)
}

// Lock sets a lock for the given key. Acquired lock is only for the key in this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (dm *DMap) Lock(key string, deadline time.Duration) (*LockContext, error) {
	return dm.lockKey(key, nilTimeout, deadline)
}

// leaseKey tries to update the expiry of the key by verifying token.
func (dm *DMap) leaseKey(key string, token []byte, timeout time.Duration) error {
	lkey := dm.name + key
	// Only one unlockKey should work for a given key.
	dm.s.locker.Lock(lkey)
	defer func() {
		err := dm.s.locker.Unlock(lkey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", key, dm.name, err)
		}
	}()

	// get the key to check its value
	entry, err := dm.get(key)
	if errors.Is(err, ErrKeyNotFound) {
		return ErrNoSuchLock
	}
	if err != nil {
		return err
	}

	// the lock is released by the node(timeout) or the user
	if !bytes.Equal(entry.Value(), token) {
		return ErrNoSuchLock
	}

	// already expired
	if (time.Now().UnixNano() / 1000000) >= entry.TTL() {
		return ErrNoSuchLock
	}

	// update
	err = dm.Expire(key, timeout)
	if err != nil {
		return fmt.Errorf("lease failed: %w", err)
	}
	return nil
}

// Lease takes key and token and tries to update the expiry with duration.
// It redirects the request to the partition owner, if required.
func (dm *DMap) lease(key string, token []byte, timeout time.Duration) error {
	hkey := partitions.HKey(dm.name, key)
	member := dm.s.primary.PartitionByHKey(hkey).Owner()
	if member.CompareByName(dm.s.rt.This()) {
		return dm.leaseKey(key, token, timeout)
	}

	cmd := protocol.NewLockLease(dm.name, key, hex.EncodeToString(token), timeout.Seconds()).Command(dm.s.ctx)
	rc := dm.s.respClient.Get(member.String())
	err := rc.Process(dm.s.ctx, cmd)
	if err != nil {
		return protocol.ConvertError(err)
	}
	return protocol.ConvertError(cmd.Err())
}

// Lease takes the duration to update the expiry for the given Lock.
func (l *LockContext) Lease(duration time.Duration) error {
	return l.dm.lease(l.key, l.token, duration)
}
