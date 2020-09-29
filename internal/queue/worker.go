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
	"context"
	"golang.org/x/sync/semaphore"
	"sync"
	"time"
)

type Worker struct {
	numWorkers int64
	semaphore *semaphore.Weighted
	consumer   func(context.Context, *Message) error
	queue      *Queue
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

func NewWorker(q *Queue, consumer func(context.Context, *Message) error, numWorkers int64) *Worker {
	ctx, cancelFunc := context.WithCancel(context.Background())
	sem := semaphore.NewWeighted(numWorkers)
	return &Worker{
		semaphore: sem,
		consumer:   consumer,
		numWorkers: numWorkers,
		queue:      q,
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
}

func (w *Worker) process(m *Message) {
	defer w.wg.Done()

}

func (w *Worker) Start() {
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			if err := w.semaphore.Acquire(w.ctx, 1); err != nil {
				continue
			}
			m, err := w.queue.Dequeue()
			if err == ErrEmptyQueue {
				continue
			}
			// TODO: Handle error
			if err := w.consumer(w.ctx, m); err != nil {
				// TODO: Log this error
				m.Nack()
				w.queue.Enqueue(m)
			}
			case <-w.ctx.Done():
			return
		}
	}
}

func (w *Worker) Close() {
	w.cancelFunc()
}
