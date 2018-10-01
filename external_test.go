// Copyright 2018 Burak Sezer
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

package olricdb

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"
	"time"
)

type customTestStruct struct {
	Field1 string
	Field2 uint64
}

func getTestVal() interface{} {
	return customTestStruct{
		Field1: "that's field1",
		Field2: 1234123,
	}
}

func testExPut(name, key, srv string, s Serializer) error {
	// Put a K/V pair.
	val := getTestVal()
	data, err := s.Marshal(val)
	if err != nil {
		return err
	}
	body := bytes.NewReader(data)
	target := fmt.Sprintf("%s/ex/put/%s/%s?t=1m", srv, name, key)
	resp, err := http.Post(target, "application/octet-stream", body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("code %d: Error: %s", resp.StatusCode, string(data))
	}
	return nil
}

func TestDMap_ExPut(t *testing.T) {
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	err = testExPut("my-dmap", "my-key", srv.URL, r.serializer)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
}

func TestDMap_ExGet(t *testing.T) {
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	// Put a K/V pair.
	err = testExPut("my-dmap", "my-key", srv.URL, r.serializer)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	// Get it again from data store.
	resp, err := http.Get(fmt.Sprintf("%s/ex/get/my-dmap/my-key", srv.URL))
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		data, derr := ioutil.ReadAll(resp.Body)
		if derr != nil {
			t.Fatalf("Expected nil. Got %v", derr)
		}
		t.Fatalf("Expected HTTP 200. Got HTTP %d: Error: %s", resp.StatusCode, string(data))
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	var val interface{}
	err = r.serializer.Unmarshal(data, &val)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if val != getTestVal() {
		t.Fatalf("Expected %v. Got: %v", getTestVal(), val)
	}
}

func TestDMap_ExDelete(t *testing.T) {
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	// Put a K/V pair.
	err = testExPut("my-dmap", "my-key", srv.URL, r.serializer)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	// Get it again from data store.
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/ex/delete/my-dmap/my-key", srv.URL), nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		data, derr := ioutil.ReadAll(resp.Body)
		if derr != nil {
			t.Fatalf("Expected nil. Got %v", derr)
		}
		t.Fatalf("Expected HTTP 200. Got HTTP %d: Error: %s", resp.StatusCode, string(data))
	}

	// Get it again from data store.
	resp, err = http.Get(fmt.Sprintf("%s/ex/get/my-dmap/my-key", srv.URL))
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected HTTP 404. Got HTTP %d", resp.StatusCode)
	}
}

func TestDMap_ExPutWithTwoMembers(t *testing.T) {
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()

	for i := 0; i < 100; i++ {
		err = testExPut("my-dmap", strconv.Itoa(i), srv1.URL, r1.serializer)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		// Get it again from data store.
		resp, err := http.Get(fmt.Sprintf("%s/ex/get/my-dmap/%s", srv2.URL, strconv.Itoa(i)))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected HTTP 200. Got HTTP %d", resp.StatusCode)
		}
	}
}

func TestDMap_ExPutExWithTwoMembers(t *testing.T) {
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()

	for i := 0; i < 100; i++ {
		// Put a K/V pair.
		val := getTestVal()
		data, err := r1.serializer.Marshal(val)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		body := bytes.NewReader(data)
		target := fmt.Sprintf("%s/ex/put/my-dmap/%d?t=1ms", srv1.URL, i)
		resp, err := http.Post(target, "application/octet-stream", body)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Expected nil. Got %v", err)
			}
			t.Errorf("Expected HTTP 200. Got HTTP %d: Error: %s", resp.StatusCode, string(data))
		}
	}
	time.Sleep(2 * time.Millisecond)
	for i := 0; i < 100; i++ {
		// Get it again from data store.
		resp, err := http.Get(fmt.Sprintf("%s/ex/get/my-dmap/%s", srv2.URL, strconv.Itoa(i)))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("Expected HTTP 404. Got HTTP %d", resp.StatusCode)
		}
	}
}

func TestDMap_ExLockWithTimeout(t *testing.T) {
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	// Put a K/V pair.
	err = testExPut("my-dmap", "my-key", srv.URL, r.serializer)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("%s/ex/lock-with-timeout/my-dmap/my-key?t=1m", srv.URL))
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected HTTP 200. Got HTTP %d", resp.StatusCode)
	}
	hkey := r.getHKey("my-dmap", "my-key")
	part := r.getPartition(hkey)
	part.RLock()
	defer part.RUnlock()
	dm, ok := part.m["my-dmap"]
	if !ok {
		t.Fatalf("my-dmap could not be found")
	}
	_, ok = dm.d[hkey]
	if !ok {
		t.Fatalf("my-key could not be found")
	}
	_, ok = dm.locker.locks["my-key"]
	if !ok {
		t.Fatalf("lock could not be found for my-key")
	}
}

func TestDMap_ExUnlock(t *testing.T) {
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	// Put a K/V pair.
	err = testExPut("my-dmap", "my-key", srv.URL, r.serializer)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("%s/ex/lock-with-timeout/my-dmap/my-key?t=1m", srv.URL))
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected HTTP 200. Got HTTP %d", resp.StatusCode)
	}

	resp, err = http.Get(fmt.Sprintf("%s/ex/unlock/my-dmap/my-key?t=1m", srv.URL))
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected HTTP 200. Got HTTP %d", resp.StatusCode)
	}
}

func TestDMap_ExDestroy(t *testing.T) {
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	// Put a K/V pair.
	err = testExPut("my-dmap", "my-key", srv.URL, r.serializer)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("%s/ex/destroy/my-dmap", srv.URL))
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected HTTP 200. Got HTTP %d", resp.StatusCode)
	}

	resp, err = http.Get(fmt.Sprintf("%s/ex/get/my-dmap/my-key", srv.URL))
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected HTTP 404. Got HTTP %d", resp.StatusCode)
	}
}
