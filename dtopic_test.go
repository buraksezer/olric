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

package olric

import (
	"context"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil/assert"
)

func TestOlric_DTopic_OrderedDelivery(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	_, err = db.NewDTopic("mydtopic", 0, OrderedDelivery)
	assert.Error(t, ErrNotImplemented, err)
}

func TestOlric_DTopic_Publish(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dt, err := db.NewDTopic("mydtopic", 0, UnorderedDelivery)
	assert.NoError(t, err)
	err = dt.Publish("my-message")
	assert.NoError(t, err)
}

func TestOlric_DTopic_AddListener(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dt, err := db.NewDTopic("mydtopic", 0, UnorderedDelivery)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	listenerID, err := dt.AddListener(func(msg DTopicMessage) {
		defer cancel()
		assert.Equal(t, "my-message", msg.Message)
		assert.NotEqual(t, 0, msg.PublishedAt)
		assert.NotEqual(t, "", msg.PublisherAddr)
	})
	assert.NoError(t, err)
	assert.NotEqual(t, 0, listenerID)

	err = dt.Publish("my-message")
	assert.NoError(t, err)

	<-ctx.Done()
	assert.Error(t, context.Canceled, ctx.Err())
}

func TestOlric_DTopic_RemoveListener(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dt, err := db.NewDTopic("mydtopic", 0, UnorderedDelivery)
	assert.NoError(t, err)

	listenerID, err := dt.AddListener(func(_ DTopicMessage) {})
	assert.NoError(t, err)
	assert.NotEqual(t, 0, listenerID)

	err = dt.RemoveListener(listenerID)
	assert.NoError(t, err)
}

func TestOlric_DTopic_Destroy(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dt, err := db.NewDTopic("mydtopic", 0, UnorderedDelivery)
	assert.NoError(t, err)

	err = dt.Destroy()
	assert.NoError(t, err)
}
