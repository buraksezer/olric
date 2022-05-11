package transaction

import (
	"errors"
	"sync"
	"time"
)

var ErrTransactionAbort = errors.New("transaction abort")

type latestTx struct {
	committedVersion uint32
	timestamp        int64
}

func (l *latestTx) isExpired(now int64) bool {
	return l.timestamp < now
}

type SSI struct {
	mtx          sync.RWMutex
	transactions map[uint64]*latestTx
}

func NewSSI() *SSI {
	return &SSI{
		transactions: make(map[uint64]*latestTx),
	}
}

func (s *SSI) Commit(readVersion, commitVersion uint32, commands []Command) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	now := time.Now().UnixNano()
	for _, cmd := range commands {
		switch cmd.Kind() {
		case ReadCommandKind:
			ltx, ok := s.transactions[cmd.HKey()]
			if !ok {
				continue
			}
			if ltx.isExpired(now) {
				continue
			}
			if ltx.committedVersion > readVersion {
				return ErrTransactionAbort
			}
		case MutateCommandKind:
			ltx, ok := s.transactions[cmd.HKey()]
			if !ok {
				continue
			}
			if ltx.isExpired(now) {
				continue
			}
			if ltx.committedVersion > readVersion {
				return ErrTransactionAbort
			}
		}
	}

	timestamp := time.Now().Add(5 * time.Second).UnixNano()
	for _, cmd := range commands {
		if cmd.Kind() == MutateCommandKind {
			s.transactions[cmd.HKey()] = &latestTx{
				committedVersion: commitVersion,
				timestamp:        timestamp,
			}
		}
	}

	return nil
}

var _ Tx = (*SSI)(nil)
