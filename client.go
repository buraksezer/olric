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

type dmapConfig struct {
}

type DMapOption func(*dmapConfig)

type DMap interface {
	// Name exposes name of the DMap.
	Name() string

	// Put sets the value for the given key. It overwrites any previous value for
	// that key, and it's thread-safe. The key has to be string. value type is arbitrary.
	// It is safe to modify the contents of the arguments after Put returns but not before.
	Put(ctx context.Context, key string, value interface{}, options ...PutOption) error

	Get(ctx context.Context, key string) (*GetResponse, error)
	Delete(ctx context.Context, key string) error
	Incr(ctx context.Context, key string, delta int) (int, error)
	Decr(ctx context.Context, key string, delta int) (int, error)
	GetPut(ctx context.Context, key string, value interface{}) (*GetResponse, error)
	Expire(ctx context.Context, key string, timeout time.Duration) error

	// Lock sets a lock for the given key. Acquired lock is only for the key in this dmap.
	//
	// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
	//
	// You should know that the locks are approximate, and only to be used for non-critical purposes.
	Lock(ctx context.Context, key string, deadline time.Duration) (LockContext, error)

	// LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time,
	// it automatically releases the lock. Acquired lock is only for the key in this dmap.
	//
	// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
	//
	// You should know that the locks are approximate, and only to be used for non-critical purposes.
	LockWithTimeout(ctx context.Context, key string, timeout, deadline time.Duration) (LockContext, error)

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
	Close(ctx context.Context) error
}
