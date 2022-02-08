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

package dmap

import (
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDMap_Balance_Invalid_PartID(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	fp := &fragmentPack{
		PartID:  12312,
		Kind:    partitions.PRIMARY,
		Name:    "foobar",
		Payload: nil,
	}
	err := s.validateFragmentPack(fp)
	require.Error(t, err)
}

func TestDMap_Balance_FragmentMergeFunction(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	require.NoError(t, err)

	err = dm.Put("mykey", "myval")
	require.NoError(t, err)

	hkey := partitions.HKey("mymap", "mykey")
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadFragment(part)
	require.NoError(t, err)

	currentValue := []byte("current-value")
	e := dm.engine.NewEntry()
	e.SetKey("mykey")
	e.SetTimestamp(time.Now().UnixNano())
	e.SetValue(currentValue)

	err = dm.fragmentMergeFunction(f, hkey, e)
	require.NoError(t, err)

	winner, err := f.storage.Get(hkey)
	require.NoError(t, err)
	require.Equal(t, currentValue, winner.Value())
}

func TestDMap_Balancer_JoinNewNode(t *testing.T) {
	cluster := testcluster.New(NewService)
	db1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := db1.NewDMap("mymap")
	require.NoError(t, err)

	var totalKeys = 1000
	for i := 0; i < totalKeys; i++ {
		key := "balancer-test." + strconv.Itoa(i)
		err = dm.Put(key, testutil.ToVal(i))
		require.NoError(t, err)
	}

	// This is an integration test. Here we try to observe the behavior of
	// balancer with the DMap service.

	db2 := cluster.AddMember(nil).(*Service) // This automatically syncs the cluster.

	var db1TotalKeys int
	for partID := uint64(0); partID < db1.config.PartitionCount; partID++ {
		part := db1.primary.PartitionByID(partID)
		db1TotalKeys += part.Length()
	}
	require.Less(t, db1TotalKeys, totalKeys)

	var db2TotalKeys int
	for partID := uint64(0); partID < db2.config.PartitionCount; partID++ {
		part := db2.primary.PartitionByID(partID)
		db2TotalKeys += part.Length()
	}
	require.Less(t, db2TotalKeys, totalKeys)

	require.Equal(t, totalKeys, db1TotalKeys+db2TotalKeys)
}

func TestDMap_Balancer_WrongOwnership(t *testing.T) {
	cluster := testcluster.New(NewService)
	db1 := cluster.AddMember(nil).(*Service)
	db2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var id uint64
	for partID := uint64(0); partID < db2.config.PartitionCount; partID++ {
		part := db2.primary.PartitionByID(partID)
		if part.Owner().CompareByID(db2.rt.This()) {
			id = part.ID()
			break
		}
	}
	fp := &fragmentPack{
		PartID: id,
		Kind:   partitions.PRIMARY,
	}
	// invalid argument: partID: 1 (kind: Primary) doesn't belong to 127.0.0.1:62096
	require.Error(t, db1.validateFragmentPack(fp))
}
