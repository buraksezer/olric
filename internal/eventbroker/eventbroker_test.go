// Copyright 2018-2021 Burak Sezer
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

package eventbroker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/olric/events"
	"github.com/buraksezer/olric/internal/dtopic"
	"github.com/buraksezer/olric/internal/testcluster"
)

type testEvent struct {
	body string
}

func TestEventSource_Publish(t *testing.T) {
	cluster := testcluster.New(dtopic.NewService)
	s := cluster.AddMember(nil).(*dtopic.Service)
	defer cluster.Shutdown()

	e := New(s)

	te := testEvent{
		body: "test event",
	}

	ctx, cancel := context.WithCancel(context.Background())
	err := e.Register("internal.events.test", func(msg events.Event) {
		defer cancel()

		if msg.Message != te {
			t.Errorf("Expected %v. Got: %v", te, msg.Message)
		}
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	es, err := e.GetEventSource("internal.events.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = es.Publish(te)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(time.Minute):
		t.Fatal("Missing event")
	}
}

func TestEventSource_Unregister(t *testing.T) {
	cluster := testcluster.New(dtopic.NewService)
	s := cluster.AddMember(nil).(*dtopic.Service)
	defer cluster.Shutdown()

	e := New(s)
	err := e.Register("internal.events.test", func(msg events.Event) {})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = e.Unregister("internal.events.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.source) > 0 {
		t.Fatalf("Number of event sources is bigger than zero: %d", len(e.source))
	}
}

func TestEventSource_Shutdown(t *testing.T) {
	cluster := testcluster.New(dtopic.NewService)
	s := cluster.AddMember(nil).(*dtopic.Service)
	defer cluster.Shutdown()

	e := New(s)
	for i := 0; i < 100; i++ {
		err := e.Register(
			fmt.Sprintf("internal.events.test-%d", i),
			func(msg events.Event) {},
		)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	err := e.Shutdown()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.source) > 0 {
		t.Fatalf("Number of event sources is bigger than zero: %d", len(e.source))
	}
}
