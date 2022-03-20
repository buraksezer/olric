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
	"errors"
	"github.com/buraksezer/olric/internal/dmap"
)

var (
	// ErrKeyNotFound means that returned when a key could not be found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyFound means that the requested key found in the cluster.
	ErrKeyFound = errors.New("key found")

	// ErrWriteQuorum means that write quorum cannot be reached to operate.
	ErrWriteQuorum = errors.New("write quorum cannot be reached")

	// ErrReadQuorum means that read quorum cannot be reached to operate.
	ErrReadQuorum = errors.New("read quorum cannot be reached")

	// ErrLockNotAcquired is returned when the requested lock could not be acquired
	ErrLockNotAcquired = errors.New("lock not acquired")

	// ErrNoSuchLock is returned when the requested lock does not exist
	ErrNoSuchLock = errors.New("no such lock")

	// ErrClusterQuorum means that the cluster could not reach a healthy numbers of members to operate.
	ErrClusterQuorum = errors.New("cannot be reached cluster quorum to operate")

	// ErrKeyTooLarge means that the given key is too large to process.
	// Maximum length of a key is 256 bytes.
	ErrKeyTooLarge = errors.New("key too large")
)

func convertDMapError(err error) error {
	switch {
	case errors.Is(err, dmap.ErrKeyFound):
		return ErrKeyFound
	case errors.Is(err, dmap.ErrKeyNotFound):
		return ErrKeyNotFound
	case errors.Is(err, dmap.ErrDMapNotFound):
		return ErrKeyNotFound
	case errors.Is(err, dmap.ErrLockNotAcquired):
		return ErrLockNotAcquired
	case errors.Is(err, dmap.ErrNoSuchLock):
		return ErrNoSuchLock
	case errors.Is(err, dmap.ErrReadQuorum):
		return ErrReadQuorum
	case errors.Is(err, dmap.ErrWriteQuorum):
		return ErrWriteQuorum
	case errors.Is(err, dmap.ErrServerGone):
		return ErrServerGone
	default:
		return convertClusterError(err)
	}
}
