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

package queue

import (
	"testing"
)

func TestQueue(t *testing.T) {
	q := New()
	payload := "message"
	q.Enqueue(payload)
	m, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	t.Run("Check Payload", func(t *testing.T) {
		if m.Payload().(string) != payload {
			t.Fatalf("Expected payload: %v. Got: %v", payload, m.Payload())
		}
	})

	t.Run("Check Status InQueue", func(t *testing.T) {
		if m.Status() != InQueue {
			t.Fatalf("Expected status InQueue: %d. Got: %d", m.Status(), InQueue)
		}
	})

	t.Run("Check Status Ack", func(t *testing.T) {
		m.Ack()
		if m.Status() != Ack {
			t.Fatalf("Expected status Ack: %d. Got: %d", m.Status(), Ack)
		}
	})

	t.Run("Check Status Nack", func(t *testing.T) {
		m.Nack()
		if m.Status() != Nack {
			t.Fatalf("Expected status Nack: %d. Got: %d", m.Status(), Nack)
		}
	})
}


func TestQueue_Messages(t *testing.T) {
	q := New()
	for i:=0; i<100; i++ {
		q.Enqueue(i)
	}

	if q.Len() != 100 {
		t.Fatalf("Expected queue length: 100. Got: %d", 100)
	}

	for i:=0; i<100; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if msg.Payload().(int) != i {
			t.Fatalf("Expected queue length: %d. Got: %v", i, msg.Payload())
		}
	}
}