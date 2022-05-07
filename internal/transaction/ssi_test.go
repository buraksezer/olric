package transaction

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransaction_SSI_Commit_Single_Transaction(t *testing.T) {
	var readVersion, commitVersion = uint32(0), uint32(1)

	s := NewSSI()
	commands := []Command{
		NewReadCommand("a"),
		NewReadCommand("b"),
		NewMutateCommand("a"),
		NewMutateCommand("b"),
		NewMutateCommand("c"),
	}
	err := s.Commit(readVersion, commitVersion, commands)
	require.NoError(t, err)
}

func TestTransaction_SSI_Concurrent_Tx(t *testing.T) {
	/*
		Description: Tx-1 starts reading keys before Tx-2 has been started and commits before it.
		Result: Tx-2 has to be aborted.
		Diagram:
				Tx-1:
					* Vr = 0
					* Vc = 1
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)
				Tx-2:
					* Vr = 0
					* Vc = 2
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)

				Tx-1: |------------------|
				Tx-2:      |----------------|
	*/
	var readVersion1, commitVersion1 = uint32(0), uint32(1)

	s := NewSSI()

	// First TX
	commands1 := []Command{
		NewReadCommand("a"),
		NewReadCommand("b"),
		NewMutateCommand("a"),
		NewMutateCommand("b"),
		NewMutateCommand("c"),
	}

	// Start a second TX before committing the first TX.

	// Second TX
	commands2 := []Command{
		NewReadCommand("a"),
		NewReadCommand("b"),
		NewMutateCommand("a"),
		NewMutateCommand("b"),
		NewMutateCommand("c"),
	}

	// Commit the first transaction.
	err := s.Commit(readVersion1, commitVersion1, commands1)
	require.NoError(t, err)

	// Try to commit the second one. It will return ErrTransactionAbort.
	var readVersion2, commitVersion2 = uint32(0), uint32(2)
	err = s.Commit(readVersion2, commitVersion2, commands2)
	require.ErrorIs(t, err, ErrTransactionAbort)
}

func TestTransaction_SSI_Sequential_Tx(t *testing.T) {
	/*
		Description: Tx-1 and Tx-2 have been started respectively.
		Result: Both of the transactions have been committed.
		Diagram:
				Tx-1:
					* Vr = 0
					* Vc = 1
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)
				Tx-2:
					* Vr = 0
					* Vc = 2
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)

				Tx-1: |------------------|
				Tx-2:                     |----------------|
	*/

	var readVersion1, commitVersion1 = uint32(0), uint32(1)

	s := NewSSI()

	// First TX
	commands1 := []Command{
		NewReadCommand("a"),
		NewReadCommand("b"),
		NewMutateCommand("a"),
		NewMutateCommand("b"),
		NewMutateCommand("c"),
	}

	// Commit the first transaction.
	err := s.Commit(readVersion1, commitVersion1, commands1)
	require.NoError(t, err)

	// Second TX
	commands2 := []Command{
		NewReadCommand("a"),
		NewReadCommand("b"),
		NewMutateCommand("a"),
		NewMutateCommand("b"),
		NewMutateCommand("c"),
	}

	var readVersion2, commitVersion2 = uint32(1), uint32(2)
	err = s.Commit(readVersion2, commitVersion2, commands2)
	require.NoError(t, err)
}

func TestTransaction_SSI_Concurrent_Many_Tx(t *testing.T) {
	/*
		Description: Tx-1 starts reading keys before Tx-2 has been started and commits before it.
		Result: Tx-2 has to be aborted.
		Diagram:
				Tx-1:
					* Vr = 0
					* Vc = 1
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)
				Tx-2:
					* Vr = 0
					* Vc = 2
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)

				Tx-1: |------------------|
				Tx-2:      |----------------|
	*/
	var numAborts, numSuccess uint32
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	s := NewSSI()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(commitVersion uint32) {
			defer wg.Done()

			<-ctx.Done()
			commands := []Command{
				NewReadCommand("a"),
				NewReadCommand("b"),
				NewMutateCommand("a"),
				NewMutateCommand("b"),
				NewMutateCommand("c"),
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

	cancel()
	wg.Wait()
	require.Equal(t, uint32(99), atomic.LoadUint32(&numAborts))
	require.Equal(t, uint32(1), atomic.LoadUint32(&numSuccess))
}

func TestTransaction_SSI_Concurrent_Many_Mutation_Tx(t *testing.T) {
	/*
		Description: Tx-1 starts reading keys before Tx-2 has been started and commits before it.
		Result: Tx-2 has to be aborted.
		Diagram:
				Tx-1:
					* Vr = 0
					* Vc = 1
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)
				Tx-2:
					* Vr = 0
					* Vc = 2
					* Commands: Get(a), Get(b), Set(a, a*2), Set(b, b*2), Set(c, a+b)

				Tx-1: |------------------|
				Tx-2:      |----------------|
	*/

	var mtx sync.Mutex
	var commitVersions []uint32
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	s := NewSSI()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(commitVersion uint32) {
			defer wg.Done()

			<-ctx.Done()
			commands := []Command{
				NewMutateCommand("a"),
				NewMutateCommand("b"),
				NewMutateCommand("c"),
			}
			// Commit the first transaction.
			err := s.Commit(0, commitVersion, commands)
			if err == nil {
				mtx.Lock()
				commitVersions = append(commitVersions, commitVersion)
				mtx.Unlock()
			}
		}(uint32(i))
	}

	cancel()
	wg.Wait()

	check := make([]uint32, len(commitVersions))
	for i, commitVersion := range commitVersions {
		check[i] = commitVersion
	}
	sort.Slice(check, func(i, j int) bool { return commitVersions[i] < commitVersions[j] })
	require.Equal(t, check, commitVersions)
}
