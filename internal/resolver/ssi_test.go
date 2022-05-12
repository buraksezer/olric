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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransaction_SSI_Commit_Single_Transaction(t *testing.T) {
	var readVersion, commitVersion = uint32(0), uint32(1)

	s := NewSSI(5*time.Second, 15*time.Second)
	commands := []*Key{
		NewKey("a", ReadCommandKind),
		NewKey("b", ReadCommandKind),
		NewKey("a", MutateCommandKind),
		NewKey("b", MutateCommandKind),
		NewKey("c", MutateCommandKind),
	}
	err := s.Commit(readVersion, commitVersion, commands)
	require.NoError(t, err)
}

func TestTransaction_SSI_Concurrent_Tx(t *testing.T) {
	/*
		Description: Tx-1 and Tx-2 are concurrent transactions. Tx-1 starts slightly before Tx-2, and it also commits before it.
		Result: Tx-2 has to be aborted. Because Tx-1 updates the latest commit version for a, b and c.
		Diagram:
				Tx-1:
					* Vr = 12
					* Vc = 32
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)
				Tx-2:
					* Vr = 23
					* Vc = 45
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)

				Tx-1: (Vr) |------------------| (Vc)
				Tx-2:      (Vr) |--------------------| (Vc)
	*/
	var readVersion1, commitVersion1 = uint32(12), uint32(32)

	s := NewSSI(5*time.Second, 15*time.Second)

	// Concurrent transactions

	// First TX
	tx1 := []*Key{
		NewKey("a", ReadCommandKind),
		NewKey("b", ReadCommandKind),
		NewKey("a", MutateCommandKind),
		NewKey("b", MutateCommandKind),
		NewKey("c", MutateCommandKind),
	}

	// Start a second TX before committing the first TX.

	// Second TX
	tx2 := []*Key{
		NewKey("a", ReadCommandKind),
		NewKey("b", ReadCommandKind),
		NewKey("a", MutateCommandKind),
		NewKey("b", MutateCommandKind),
		NewKey("c", MutateCommandKind),
	}

	// Commit the first transaction.
	err := s.Commit(readVersion1, commitVersion1, tx1)
	require.NoError(t, err)

	// Try to commit the second one. It will return ErrTransactionAbort.
	var readVersion2, commitVersion2 = uint32(23), uint32(45)
	err = s.Commit(readVersion2, commitVersion2, tx2)
	require.ErrorIs(t, err, ErrTransactionAbort)
}

func TestTransaction_SSI_Concurrent_Tx2(t *testing.T) {
	/*
		Description: Tx-1 and Tx-2 are concurrent transactions. Tx-1 starts before Tx-2, and Tx-2 commits before Tx-1.
		Result: Tx-1 has to be aborted.
		Diagram:
				Tx-1:
					* Vr = 10
					* Vc = 42
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)
				Tx-2:
					* Vr = 18
					* Vc = 35
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)

				Tx-1: (Vr) |------------------------------| (Vc)
				Tx-2:      (Vr) |--------------------| (Vc)
	*/
	var readVersion1, commitVersion1 = uint32(10), uint32(42)

	s := NewSSI(5*time.Second, 15*time.Second)

	// Concurrent transactions

	// First TX
	tx1 := []*Key{
		NewKey("a", ReadCommandKind),
		NewKey("b", ReadCommandKind),
		NewKey("a", MutateCommandKind),
		NewKey("b", MutateCommandKind),
		NewKey("c", MutateCommandKind),
	}

	// Start a second TX before committing the first TX.

	// Second TX
	tx2 := []*Key{
		NewKey("a", ReadCommandKind),
		NewKey("b", ReadCommandKind),
		NewKey("a", MutateCommandKind),
		NewKey("b", MutateCommandKind),
		NewKey("c", MutateCommandKind),
	}

	// Commit Tx-2 transaction.
	var readVersion2, commitVersion2 = uint32(18), uint32(35)
	err := s.Commit(readVersion2, commitVersion2, tx2)
	require.NoError(t, err)

	// Try to commit Tx-1. It will return ErrTransactionAbort.
	err = s.Commit(readVersion1, commitVersion1, tx1)
	require.ErrorIs(t, err, ErrTransactionAbort)
}

func TestTransaction_SSI_Sequential_Tx(t *testing.T) {
	/*
		Description: Tx-1 and Tx-2 are not concurrent transactions.
		Result: Both of the transactions have to be committed.
		Diagram:
				Tx-1:
					* Vr = 10
					* Vc = 20
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)
				Tx-2:
					* Vr = 23
					* Vc = 33
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)

				Tx-1: (Vr) |---------------| (Vc)
				Tx-2:                        (Vr) |--------------| (Vc)
	*/
	var readVersion1, commitVersion1 = uint32(10), uint32(20)

	s := NewSSI(5*time.Second, 15*time.Second)

	// Concurrent transactions

	// Tx-1
	tx1 := []*Key{
		NewKey("a", ReadCommandKind),
		NewKey("b", ReadCommandKind),
		NewKey("a", MutateCommandKind),
		NewKey("b", MutateCommandKind),
		NewKey("c", MutateCommandKind),
	}
	err := s.Commit(readVersion1, commitVersion1, tx1)
	require.NoError(t, err)

	// Tx-2
	tx2 := []*Key{
		NewKey("a", ReadCommandKind),
		NewKey("b", ReadCommandKind),
		NewKey("a", MutateCommandKind),
		NewKey("b", MutateCommandKind),
		NewKey("c", MutateCommandKind),
	}

	// Commit the second transaction.
	var readVersion2, commitVersion2 = uint32(23), uint32(33)
	err = s.Commit(readVersion2, commitVersion2, tx2)
	require.NoError(t, err)
}

func TestTransaction_SSI_Concurrent_Txs_With_Different_Keys(t *testing.T) {
	/*
		Description: Tx-1 starts reading keys before Tx-2 has been started and commits before it. They work on different set of keys.
		Result: Both of the transactions have to be committed.
		Diagram:
				Tx-1:
					* Vr = 10
					* Vc = 42
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)
				Tx-2:
					* Vr = 18
					* Vc = 35
					* Commands: Get(x), Get(y), Set(x, x*2), Set(y, y*2), Set(z, x+y)

				Tx-1: (Vr) |------------------------------| (Vc)
				Tx-2:      (Vr) |--------------------| (Vc)
	*/

	s := NewSSI(5*time.Second, 15*time.Second)

	// Concurrent transactions

	// Tx-1
	var readVersion1, commitVersion1 = uint32(10), uint32(42)
	tx1 := []*Key{
		NewKey("a", ReadCommandKind),
		NewKey("b", ReadCommandKind),
		NewKey("a", MutateCommandKind),
		NewKey("b", MutateCommandKind),
		NewKey("c", MutateCommandKind),
	}

	// Start a second TX before committing the first TX.

	// Tx-2
	var readVersion2, commitVersion2 = uint32(18), uint32(35)
	tx2 := []*Key{
		NewKey("x", ReadCommandKind),
		NewKey("y", ReadCommandKind),
		NewKey("x", MutateCommandKind),
		NewKey("y", MutateCommandKind),
		NewKey("z", MutateCommandKind),
	}

	// Commit the second transaction.
	err := s.Commit(readVersion2, commitVersion2, tx2)
	require.NoError(t, err)

	// Try to commit the first one.
	err = s.Commit(readVersion1, commitVersion1, tx1)
	require.NoError(t, err)
}

func TestTransaction_SSI_Concurrent_Many_Tx(t *testing.T) {
	/*
		Description: Start 100 transactions concurrently and control the flow with a context.
		Result: Only one of the transactions will be committed.
		Diagram:
				Tx-1:
					* Vr = 0
					* Vc = 1
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)
				Tx-2:
					* Vr = 0
					* Vc = 2
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)

				Tx-1:   |------------------|
				Tx-2:   |------------------|
							.......
				Tx-100: |------------------|
	*/
	var numAborts, numSuccess uint32
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	s := NewSSI(5*time.Second, 15*time.Second)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(commitVersion uint32) {
			defer wg.Done()

			<-ctx.Done()
			commands := []*Key{
				NewKey("a", ReadCommandKind),
				NewKey("b", ReadCommandKind),
				NewKey("a", MutateCommandKind),
				NewKey("b", MutateCommandKind),
				NewKey("c", MutateCommandKind),
			}
			// Commit the first transaction.
			err := s.Commit(0, commitVersion, commands)
			if err == ErrTransactionAbort {
				atomic.AddUint32(&numAborts, 1)
			} else if err == nil {
				atomic.AddUint32(&numSuccess, 1)
			}
		}(uint32(i))
	}

	// All transactions will start processing.
	cancel()

	wg.Wait()

	require.Equal(t, uint32(99), atomic.LoadUint32(&numAborts))
	require.Equal(t, uint32(1), atomic.LoadUint32(&numSuccess))
}

func TestTransaction_SSI_Concurrent_Many_T(t *testing.T) {
	var wg sync.WaitGroup

	s := NewSSI(250*time.Millisecond, 500*time.Millisecond)
	s.Start()
	defer s.Stop()

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(commitVersion uint32) {
			defer wg.Done()

			commands := []*Key{
				NewKey(fmt.Sprintf("key-%d", commitVersion), MutateCommandKind),
			}
			// Commit the first transaction.
			err := s.Commit(commitVersion-1, commitVersion, commands)
			if err != nil {
				fmt.Println(err)
			}
		}(uint32(i))
	}

	wg.Wait()

	<-time.After(time.Second)

	s.mtx.Lock()
	require.Equal(t, 0, len(s.transactions))
	s.mtx.Unlock()
}
