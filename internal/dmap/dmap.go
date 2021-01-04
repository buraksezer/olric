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

package dmap

import (
	"errors"
	"time"
)

const nilTimeout = 0 * time.Second

// ErrKeyNotFound is returned when a key could not be found.
var ErrKeyNotFound = errors.New("key not found")
var ErrDMapNotFound = errors.New("dmap not found")

// DMap defines the internal representation of a dmap.
type DMap struct {
	name   string
	s      *Service
	config *configuration
}

func (s *Service) LoadDMap(name string) (*DMap, error) {
	s.RLock()
	defer s.RUnlock()

	dm, ok := s.dmaps[name]
	if !ok {
		return nil, ErrDMapNotFound
	}
	return dm, nil
}

// NewDMap creates an returns a new DMap instance.
func (s *Service) NewDMap(name string) (*DMap, error) {
	// Check operation status first:
	//
	// * Checks member count in the cluster, returns ErrClusterQuorum if
	//   the quorum value cannot be satisfied,
	// * Checks bootstrapping status and awaits for a short period before
	//   returning ErrRequest timeout.
	if err := s.rt.CheckMemberCountQuorum(); err != nil {
		return nil, err
	}
	// An Olric node has to be bootstrapped to function properly.
	if err := s.rt.CheckBootstrap(); err != nil {
		return nil, err
	}

	s.Lock()
	defer s.Unlock()

	dm, ok := s.dmaps[name]
	if ok {
		return dm, nil
	}

	dm = &DMap{
		config: &configuration{},
		name:   name,
		s:      s,
	}
	if err := dm.config.load(s.config, name); err != nil {
		return nil, err
	}
	s.dmaps[name] = dm
	return dm, nil
}

func timeoutToTTL(timeout time.Duration) int64 {
	if timeout.Seconds() == 0 {
		return 0
	}
	// convert nanoseconds to milliseconds
	return (timeout.Nanoseconds() + time.Now().UnixNano()) / 1000000
}

func isKeyExpired(ttl int64) bool {
	if ttl == 0 {
		return false
	}
	// convert nanoseconds to milliseconds
	return (time.Now().UnixNano() / 1000000) >= ttl
}
