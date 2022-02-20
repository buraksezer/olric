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

package olric

import (
	"context"
	"time"

	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/stats"
)

const DefaultScanCount = 10

type Iterator interface {
	Next() bool
	Key() string
	Close()
}

type LockContext interface {
	Unlock(ctx context.Context) error
	Lease(ctx context.Context, duration time.Duration) error
}

type PutOption func(*dmap.PutConfig)

func EX(ex time.Duration) PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasEX = true
		cfg.EX = ex
	}
}

func PX(px time.Duration) PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasPX = true
		cfg.PX = px
	}
}

func EXAT(exat time.Duration) PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasEXAT = true
		cfg.EXAT = exat
	}
}

func PXAT(pxat time.Duration) PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasPXAT = true
		cfg.PX = pxat
	}
}

func NX() PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasNX = true
	}
}

func XX() PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasXX = true
	}
}

type dmapConfig struct{}

type DMapOption func(*dmapConfig)

type ScanOption func(*dmap.ScanConfig)

func Count(c int) ScanOption {
	return func(cfg *dmap.ScanConfig) {
		cfg.HasCount = true
		cfg.Count = c
	}
}

func Match(s string) ScanOption {
	return func(cfg *dmap.ScanConfig) {
		cfg.HasMatch = true
		cfg.Match = s
	}
}

// DMap describes a distributed map client.
type DMap interface {
	// Name exposes name of the DMap.
	Name() string

	// Put sets the value for the given key. It overwrites any previous value for
	// that key, and it's thread-safe. The key has to be string. value type is arbitrary.
	// It is safe to modify the contents of the arguments after Put returns but not before.
	Put(ctx context.Context, key string, value interface{}, options ...PutOption) error

	// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
	// does not contain the key. It's thread-safe. It is safe to modify the contents
	// of the returned value.
	Get(ctx context.Context, key string) (*GetResponse, error)

	// Delete deletes the value for the given key. Delete will not return error
	// if key doesn't exist. It's thread-safe. It is safe to modify the contents
	// of the argument after Delete returns.
	Delete(ctx context.Context, key string) error

	// Incr atomically increments key by delta. The return value is the new value
	// after being incremented or an error.
	Incr(ctx context.Context, key string, delta int) (int, error)

	// Decr atomically decrements key by delta. The return value is the new value
	// after being decremented or an error.
	Decr(ctx context.Context, key string, delta int) (int, error)

	// GetPut atomically sets key to value and returns the old value stored at key.
	GetPut(ctx context.Context, key string, value interface{}) (*GetResponse, error)

	// Expire updates the expiry for the given key. It returns ErrKeyNotFound if
	// the DB does not contain the key. It's thread-safe.
	Expire(ctx context.Context, key string, timeout time.Duration) error

	// Lock sets a lock for the given key. Acquired lock is only for the key in
	// this dmap.
	//
	// It returns immediately if it acquires the lock for the given key. Otherwise,
	// it waits until deadline.
	//
	// You should know that the locks are approximate, and only to be used for
	// non-critical purposes.
	Lock(ctx context.Context, key string, deadline time.Duration) (LockContext, error)

	// LockWithTimeout sets a lock for the given key. If the lock is still unreleased
	// the end of given period of time,
	// it automatically releases the lock. Acquired lock is only for the key in
	// this dmap.
	//
	// It returns immediately if it acquires the lock for the given key. Otherwise,
	// it waits until deadline.
	//
	// You should know that the locks are approximate, and only to be used for
	// non-critical purposes.
	LockWithTimeout(ctx context.Context, key string, timeout, deadline time.Duration) (LockContext, error)

	Scan(ctx context.Context, options ...ScanOption) (Iterator, error)

	// Destroy flushes the given dmap on the cluster. You should know that there
	// is no global lock on DMaps. So if you call Put/PutEx and Destroy methods
	// concurrently on the cluster, Put call may set new values to the dmap.
	Destroy(ctx context.Context) error
}

type statsConfig struct {
	CollectRuntime bool
}

type StatsOption func(*statsConfig)

type Client interface {
	NewDMap(name string, options ...DMapOption) (DMap, error)
	Stats(options ...StatsOption) (stats.Stats, error)
	Ping(addr string) error
	PingWithMessage(addr, message string) (string, error)
	RoutingTable(ctx context.Context) (RoutingTable, error)
	Close(ctx context.Context) error
}
