// Copyright 2018-2022 Burak Sezer
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

package resolver

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrTransactionAbort = errors.New("transaction abort")

type latestTx struct {
	commitVersion uint32
	timestamp     int64
}

func (l *latestTx) isExpired(now int64) bool {
	return l.timestamp < now
}

type SSI struct {
	mtx                         sync.RWMutex
	mvccWindow                  time.Duration
	cleanOldTransactionInterval time.Duration
	transactions                map[uint64]*latestTx
	ctx                         context.Context
	cancel                      context.CancelFunc
	wg                          sync.WaitGroup
}

func NewSSI(mvccWindow, cleanOldTransactionInterval time.Duration) *SSI {
	ctx, cancel := context.WithCancel(context.Background())
	return &SSI{
		transactions:                make(map[uint64]*latestTx),
		mvccWindow:                  mvccWindow,
		cleanOldTransactionInterval: cleanOldTransactionInterval,
		ctx:                         ctx,
		cancel:                      cancel,
	}
}

func (s *SSI) Start() {
	s.wg.Add(1)
	go s.cleanOldTransactionsPeriodically()
}

func (s *SSI) cleanOldTransactions() {
	var total int
	now := time.Now().UnixNano()

	s.mtx.Lock()
	defer s.mtx.Unlock()

	for hkey, ltx := range s.transactions {
		select {
		case <-s.ctx.Done():
			return
		default:
			if total > 1000 {
				break
			}

			if ltx.isExpired(now) {
				delete(s.transactions, hkey)
			}
			total++
		}
	}
}

func (s *SSI) cleanOldTransactionsPeriodically() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cleanOldTransactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.cleanOldTransactions()
		}
	}
}

func (s *SSI) Commit(readVersion, commitVersion uint32, keys []*Key) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	now := time.Now().UnixNano()
	for _, key := range keys {
		ltx, ok := s.transactions[key.HKey]
		if !ok {
			continue
		}
		if ltx.isExpired(now) {
			continue
		}
		if ltx.commitVersion > readVersion {
			return ErrTransactionAbort
		}
	}

	timestamp := time.Now().Add(s.mvccWindow).UnixNano()
	for _, cmd := range keys {
		if cmd.Kind == MutateCommandKind {
			s.transactions[cmd.HKey] = &latestTx{
				commitVersion: commitVersion,
				timestamp:     timestamp,
			}
		}
	}

	return nil
}

func (s *SSI) Stop() {
	select {
	case <-s.ctx.Done():
		return
	default:
	}

	s.cancel()
	s.wg.Done()
}

var _ Tx = (*SSI)(nil)
