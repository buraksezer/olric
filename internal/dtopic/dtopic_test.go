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

package dtopic

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestDTopic_Subscribe_And_Publish_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.TODO()
	pubsub := rc.Subscribe(ctx, "my-topic")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	require.NoError(t, err)

	// Go channel which receives messages.
	ch := pubsub.Channel()

	expected := make(map[string]struct{})
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("my-message-%d", i)
		err = rc.Publish(ctx, "my-topic", msg).Err()
		require.NoError(t, err)
		expected[msg] = struct{}{}
	}

	consumed := make(map[string]struct{})
L:
	for {
		select {
		case msg := <-ch:
			require.Equal(t, "my-topic", msg.Channel)
			consumed[msg.Payload] = struct{}{}
			if len(consumed) == 10 {
				// It would be OK
				break L
			}
		case <-time.After(5 * time.Second):
			// Enough. Break it and check the consumed items.
			break L
		}
	}

	require.Equal(t, expected, consumed)
}

func TestDTopic_Unsubscribe(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.TODO()
	pubsub := rc.Subscribe(ctx, "my-topic")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	require.NoError(t, err)

	// Go channel which receives messages.
	ch := pubsub.Channel()

	err = pubsub.Unsubscribe(ctx, "my-topic")
	require.NoError(t, err)

	err = rc.Publish(ctx, "my-topic", "hello, world!").Err()
	require.NoError(t, err)
L:
	for {
		select {
		case <-ch:
			require.Fail(t, "Received a message from an unsubscribed channel")
		case <-time.After(250 * time.Millisecond):
			// Enough. Break it and check the consumed items.
			break L
		}
	}
}

func TestDTopic_PSubscribe_And_Publish_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.TODO()
	pubsub := rc.PSubscribe(ctx, "h?llo")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	require.NoError(t, err)

	// Go channel which receives messages.
	ch := pubsub.Channel()

	expected := make(map[string]struct{})
	for _, topic := range []string{"hello", "hallo", "hxllo"} {
		for i := 0; i < 10; i++ {
			msg := fmt.Sprintf("my-message-%s-%d", topic, i)
			err = rc.Publish(ctx, topic, msg).Err()
			require.NoError(t, err)
			expected[msg] = struct{}{}
		}
	}

	consumed := make(map[string]struct{})
L:
	for {
		select {
		case msg := <-ch:
			consumed[msg.Payload] = struct{}{}
			if len(consumed) == 30 {
				// It would be OK
				break L
			}
		case <-time.After(5 * time.Second):
			// Enough. Break it and check the consumed items.
			break L
		}
	}

	require.Equal(t, expected, consumed)
}

func TestDTopic_PUnsubscribe(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.TODO()
	pubsub := rc.PSubscribe(ctx, "h?llo")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	require.NoError(t, err)

	// Go channel which receives messages.
	ch := pubsub.Channel()

	err = pubsub.PUnsubscribe(ctx, "h?llo")
	require.NoError(t, err)

	for _, topic := range []string{"hello", "hallo", "hxllo"} {
		err = rc.Publish(ctx, topic, "hello, world!").Err()
		require.NoError(t, err)
	}

L:
	for {
		select {
		case <-ch:
			require.Fail(t, "Received a message from an unsubscribed channel")
		case <-time.After(250 * time.Millisecond):
			// Enough. Break it and check the consumed items.
			break L
		}
	}
}

func TestDTopic_Ping(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.TODO()
	pubsub := rc.Subscribe(ctx, "my-topic")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	require.NoError(t, err)

	err = pubsub.Ping(ctx, "hello, world!")
	require.NoError(t, err)

	msg, err := pubsub.Receive(ctx)
	require.NoError(t, err)
	require.Equal(t, "Pong<hello, world!>", msg.(*redis.Pong).String())
}

func TestDTopic_Close(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.TODO()
	pubsub := rc.Subscribe(ctx, "my-topic")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	require.NoError(t, err)

	err = pubsub.Close()
	require.NoError(t, err)

	err = pubsub.Ping(ctx)
	require.Error(t, err, "redis: client is closed")
	// TODO: Control active subscriber count
}
