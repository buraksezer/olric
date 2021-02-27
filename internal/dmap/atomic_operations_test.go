package dmap

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/transport"
)

func Test_exIncrDecrOperation(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	value, err := s.serializer.Marshal(100)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	req := protocol.NewDMapMessage(protocol.OpIncr)
	req.SetDMap("mydmap")
	req.SetKey("mykey")
	req.SetValue(value)
	req.SetExtra(protocol.AtomicExtra{
		Timestamp: time.Now().UnixNano(),
	})
	cc := &config.Client{
		MaxConn: 10,
	}
	cc.Sanitize()
	c := transport.NewClient(cc)
	resp, err := c.RequestTo(s.rt.This().String(), req)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	var val interface{}
	err = s.serializer.Unmarshal(resp.Value(), &val)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(int) != 100 {
		t.Fatalf("Expected value is 100. Got: %v", val)
	}
}

func Test_exGetPutOperation(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cc := &config.Client{
		MaxConn: 100,
	}
	cc.Sanitize()
	c := transport.NewClient(cc)
	var total int64
	var wg sync.WaitGroup
	var final int64
	start := make(chan struct{})

	getput := func(i int) {
		defer wg.Done()
		<-start

		value, err := s.serializer.Marshal(i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		req := protocol.NewDMapMessage(protocol.OpGetPut)
		req.SetDMap("atomic_test")
		req.SetKey("atomic_getput")
		req.SetValue(value)
		req.SetExtra(protocol.AtomicExtra{
			Timestamp: time.Now().UnixNano(),
		})

		resp, err := c.RequestTo(s.rt.This().String(), req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if len(resp.Value()) != 0 {
			var oldval interface{}
			err = s.serializer.Unmarshal(resp.Value(), &oldval)
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}

			if oldval != nil {
				atomic.AddInt64(&total, int64(oldval.(int)))
			}
		}
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go getput(i)
		final += int64(i)
	}

	close(start)
	wg.Wait()

	dm, err := s.NewDMap("atomic_test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	result, err := dm.Get("atomic_getput")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	atomic.AddInt64(&total, int64(result.(int)))
	if atomic.LoadInt64(&total) != final {
		t.Fatalf("Expected %d. Got: %d", final, atomic.LoadInt64(&total))
	}
}
