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
	"errors"
	"sync"
	"sync/atomic"
)

var ErrEmptyQueue = errors.New("queue is empty")

const (
	InQueue = iota + 1
	Ack
	Nack
)

type Message struct {
	status  int32
	payload interface{}
}

func (m *Message) Ack() {
	atomic.StoreInt32(&m.status, Ack)
}

func (m *Message) Nack() {
	atomic.StoreInt32(&m.status, Nack)
}

func (m *Message) inQueue() {
	atomic.StoreInt32(&m.status, InQueue)
}

func (m *Message) Status() int32 {
	return atomic.LoadInt32(&m.status)
}

func (m *Message) Payload() interface{} {
	// Payload is read-only.
	return m.payload
}

func newMessage(payload interface{}) *Message {
	return &Message{
		payload: payload,
	}
}

type Queue struct {
	numWorkers int
	queue      []*Message
	mu         sync.RWMutex
}

func New() *Queue {
	return &Queue{
		queue: []*Message{},
	}
}

func (q *Queue) Enqueue(payload interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	m := newMessage(payload)
	m.inQueue()
	q.queue = append(q.queue, m)
}

func (q *Queue) Dequeue() (*Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) == 0 {
		return nil, ErrEmptyQueue
	}
	m := q.queue[0]
	copy(q.queue[0:], q.queue[1:])
	q.queue[len(q.queue)-1] = nil
	q.queue = q.queue[:len(q.queue)-1]
	return m, nil
}

func (q *Queue) Len() int {
	return len(q.queue)
}
