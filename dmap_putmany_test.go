package olric

import (
	"context"
	"fmt"
	"github.com/buraksezer/olric/serializer"
	"io/ioutil"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

const mebibyte = 1 << 20

func TestDMap_PutMany(t *testing.T) {
	const (
		keyCount  = 1000 // Number of keys to set in 1 round.
		valueSize = 10   // Value size in bytes.
		// Cumulative value memory = keyCount * valueSize
		tableSize = 10 * mebibyte

		sampleInterval = 5 * time.Second
		statusInterval = 10 * time.Second
	)

	cfg := testSingleReplicaConfig()
	t.Log(cfg.PartitionCount)
	cfg.TableSize = tableSize
	db, err := newDB(cfg)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	dm, err := db.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	before := readMem()
	t.Logf("Before Memory: %.2f MiB", toMiB(before.Alloc))
	expectedMemDiff := toMiB(keyCount * valueSize)
	t.Logf("Expected memory increase of %.2f MiB", expectedMemDiff)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go runMemStatusTicker(ctx, t, sampleInterval, statusInterval)

	value := make([]byte, valueSize)
	for i := 0; i < keyCount; i++ {
		err := dm.Put(bkey(i), value)
		if err != nil {
			panic(err)
		}
	}

	runtime.GC()
	after := readMem()
	writePprofHeap(t)

	t.Logf("After Memory: %.2f MiB", toMiB(after.Alloc))
	actualMemDiff := toMiB(after.Alloc) - toMiB(before.Alloc)
	t.Logf("Expected memory increase of %.2f MiB, got %.2f MiB. "+
		"actual/expected ratio: %.2f",
		expectedMemDiff,
		actualMemDiff,
		actualMemDiff/expectedMemDiff,
	)
}

func toMiB(i uint64) float64 {
	return float64(i) / mebibyte
}

func readMem() runtime.MemStats {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return mem
}

func writePprofHeap(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	t.Log("Pprof written to:", f.Name())
}

func max(values ...uint64) uint64 {
	maxVal := values[0]
	for _, val := range values[1:] {
		if val > maxVal {
			maxVal = val
		}
	}
	return maxVal
}

func runMemStatusTicker(ctx context.Context, t *testing.T, sampleInterval, statusInterval time.Duration) {
	sampleTick := time.NewTicker(sampleInterval)
	defer sampleTick.Stop()
	statusTick := time.NewTicker(statusInterval)
	defer statusTick.Stop()

	const maxSamples = 10
	samples := make([]uint64, maxSamples)
	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-sampleTick.C:
			mem := readMem()
			samples[i%maxSamples] = mem.Alloc
			i++
		case <-statusTick.C:
			t.Logf("Current memory: %.2f MiB", toMiB(max(samples...)))
		}
	}
}

func BenchmarkDMap_PutMany(b *testing.B) {
	b.ReportAllocs()
	const (
		valueSize = 10                      // Value size in bytes.
		tableSize = 1<<20

		sampleInterval = 5 * time.Second
		statusInterval = 10 * time.Second
	)
	cfg := testSingleReplicaConfig()
	cfg.TableSize = tableSize
	cfg.Serializer = serializer.NewMsgpackSerializer()
	cfg.LogOutput = ioutil.Discard
	db, err := newDB(cfg)
	if err != nil {
		b.Fatalf("Expected nil. Got: %v", err)
	}

	dm, err := db.NewDMap("mymap")
	if err != nil {
		b.Fatalf("Expected nil. Got: %v", err)
	}

	value := make([]byte, valueSize)
	b.ResetTimer()
	fmt.Println("Olric node:", db.this, b.N)
	for i := 0; i < b.N; i++ {
		err := dm.Put(bkey(i), value)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	b.ReportMetric(toMiB(mem.Alloc), "heap-MiB")
	b.ReportMetric(toMiB(mem.HeapInuse), "heap-inuse-MiB")
	b.ReportMetric(toMiB(mem.HeapReleased), "heap-released-MiB")

	err = db.Shutdown(context.Background())
	b.Log("Olric node is gone", db.this, err)
}
