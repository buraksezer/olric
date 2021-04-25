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

package cli

import (
	"testing"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/client"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/testolric"
)

var testConfig = &client.Config{
	Client: config.NewClient(),
}

func TestOlric_CLI_Evaluate(t *testing.T) {
	db, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	testConfig.Servers = []string{db.Addr}

	cl, err := client.New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	dm := cl.NewDMap("my-dmap")
	defer func() {
		_ = dm.Destroy()
	}()

	c, err := New(testConfig.Servers[0], "gob", "1s")
	if err != nil {
		t.Fatalf("Expected nil, Got: %v", err)
	}

	t.Run("run evalPut", func(t *testing.T) {
		fields := []string{
			"my-key",
			"my-value",
		}
		err := c.evalPut(dm, fields)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
	})

	t.Run("run evalGet", func(t *testing.T) {
		_ = dm.Put("evalGet-test", "evalGet-test")
		fields := []string{
			"evalGet-test",
		}
		err := c.evalGet(dm, fields)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
	})

	t.Run("run evalDelete", func(t *testing.T) {
		_ = dm.Put("evalDelete-test", "evalDelete-test")
		fields := []string{
			"evalDelete-test",
		}
		err := c.evalDelete(dm, fields)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
	})

	t.Run("run evalIncr invalid command", func(t *testing.T) {
		_ = dm.Put("evalIncr-test", 1)
		fields := []string{
			"evalIncr-test",
		}
		err := c.evalIncr(dm, fields)
		if err != errInvalidCommand {
			t.Fatalf("Expected errInvalidCommand, Got: %v", err)
		}
	})

	t.Run("run evalIncr", func(t *testing.T) {
		_ = dm.Put("evalIncr-test", 1)
		fields := []string{
			"evalIncr-test", // key
			"1",             // delta
		}
		err := c.evalIncr(dm, fields)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		val, err := dm.Get("evalIncr-test")
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		if val.(int) != 2 {
			t.Fatalf("Expected 2, Got: %v", val)
		}
	})

	t.Run("run evalIDecr invalid command", func(t *testing.T) {
		_ = dm.Put("evalDecr-test", 1)
		fields := []string{
			"evalDecr-test",
		}
		err := c.evalDecr(dm, fields)
		if err != errInvalidCommand {
			t.Fatalf("Expected errInvalidCommand, Got: %v", err)
		}
	})

	t.Run("run evalDecr", func(t *testing.T) {
		_ = dm.Put("evalDecr-test", 1)
		fields := []string{
			"evalDecr-test", // key
			"1",             // delta
		}
		err := c.evalDecr(dm, fields)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		val, err := dm.Get("evalDecr-test")
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		if val.(int) != 0 {
			t.Fatalf("Expected 0, Got: %v", val)
		}
	})

	t.Run("run evalDestroy", func(t *testing.T) {
		err := c.evalDestroy(dm)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
	})

	t.Run("run evalExpire invalid command", func(t *testing.T) {
		_ = dm.Put("evalExpire-test", "evalExpire-test")
		fields := []string{
			"evalExpire-test",
		}
		err := c.evalExpire(dm, fields)
		if err != errInvalidCommand {
			t.Fatalf("Expected errInvalidCommand, Got: %v", err)
		}
	})

	t.Run("run evalExpire", func(t *testing.T) {
		_ = dm.Put("evalExpire-test", "evalExpire-test")
		fields := []string{
			"evalExpire-test",
			"1ms",
		}
		err := c.evalExpire(dm, fields)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		<-time.After(2 * time.Millisecond)
		_, err = dm.Get("evalExpire-test")
		if err != olric.ErrKeyNotFound {
			t.Fatalf("Expected olric.ErrKeyNotFound, Got: %v", err)
		}
	})

	t.Run("run evalGetPut invalid command", func(t *testing.T) {
		fields := []string{
			"evalGetPut-test",
		}
		err := c.evalGetPut(dm, fields)
		if err != errInvalidCommand {
			t.Fatalf("Expected errInvalidCommand, Got: %v", err)
		}
	})

	t.Run("run evalGetPut", func(t *testing.T) {
		_ = dm.Put("evalGetPut-test", 1)
		fields := []string{
			"evalGetPut-test", // key
			"2",               // delta
		}
		err := c.evalGetPut(dm, fields)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		val, err := dm.Get("evalGetPut-test")
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		if val.(string) != "2" {
			t.Fatalf("Expected 2, Got: %v", val)
		}
	})

	t.Run("run evalPutIf invalid command", func(t *testing.T) {
		fields := []string{
			"evalPutIf-test",
		}
		err := c.evalPutIf(dm, fields)
		if err != errInvalidCommand {
			t.Fatalf("Expected errInvalidCommand, Got: %v", err)
		}
	})

	t.Run("run evalPutIf ifNotFound", func(t *testing.T) {
		_ = dm.Put("evalPutIf-test", 1)
		fields := []string{
			"evalPutIf-test", // key
			"2",              // value
			"ifNotFound",
		}
		err := c.evalPutIf(dm, fields)
		if err != olric.ErrKeyFound {
			t.Fatalf("Expected olric.ErrKeyFound, Got: %v", err)
		}
	})

	t.Run("run evalPutIf ifFound", func(t *testing.T) {
		_ = dm.Put("evalPutIf-test", 1)
		fields := []string{
			"evalPutIf-test", // key
			"2",              // value
			"ifFound",
		}
		err := c.evalPutIf(dm, fields)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		val, err := dm.Get("evalPutIf-test")
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		if val.(string) != "2" {
			t.Fatalf("Expected 2, Got: %v", val)
		}
	})

	t.Run("run evalPutIfEx invalid command", func(t *testing.T) {
		fields := []string{
			"evalPutIfEx-test",
		}
		err := c.evalPutIfEx(dm, fields)
		if err != errInvalidCommand {
			t.Fatalf("Expected errInvalidCommand, Got: %v", err)
		}
	})

	t.Run("run evalPutIfEx ifFound", func(t *testing.T) {
		_ = dm.Put("evalPutIfEx-test", 1)
		fields := []string{
			"evalPutIfEx-test", // key
			"2",                // value
			"1ms",
			"ifFound",
		}
		err := c.evalPutIfEx(dm, fields)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		<-time.After(2 * time.Millisecond)
		_, err = dm.Get("valPutIfEx-test")
		if err != olric.ErrKeyNotFound {
			t.Fatalf("Expected olric.ErrKeyNotFound, Got: %v", err)
		}
	})

	t.Run("run evalGetEntry", func(t *testing.T) {
		_ = dm.Put("evalGetEntry-test", "evalGetEntry-test")
		fields := []string{
			"evalGetEntry-test",
		}
		err := c.evalGetEntry(dm, fields)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
	})
}
