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
	"testing"
	"time"
)

func TestNewWorker(t *testing.T) {
	q := New()
	q.Enqueue(1)

	ctx, cancel := context.WithCancel(context.Background())
	f := func(ctx context.Context, _ *Message) error {
		cancel()
		return nil
	}

	w := NewWorker(q, f, 1)
	defer w.Close()

	go func() {
		w.Start()
	}()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatalf("Worker expired")
	}
}
